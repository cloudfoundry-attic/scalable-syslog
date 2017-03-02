package egress_test

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("TCP", func() {
	Describe("connecting to drain", func() {
		It("establishes connection to TCP drain", func() {
			mockDrain := newMockTCPDrain()
			_, err := egress.NewTCP(url.URL{
				Scheme: "syslog",
				Host:   mockDrain.Addr().String(),
			}, "test-app-id", "test-hostname")
			Expect(err).ToNot(HaveOccurred())
			Eventually(mockDrain.ConnCount).Should(Equal(uint64(1)))
		})

		It("does not accept schemes other than syslog", func() {
			_, err := egress.NewTCP(url.URL{
				Scheme: "https",
				Host:   "example.com:1234",
			}, "test-app-id", "test-hostname")
			Expect(err).To(HaveOccurred())
		})

		It("accepts a dialer to customize timeouts/etc", func() {
			mockDialer := newMockDialer()
			close(mockDialer.DialOutput.Conn)
			close(mockDialer.DialOutput.Err)

			_, err := egress.NewTCP(url.URL{
				Scheme: "syslog",
				Host:   "example.com:1234",
			}, "test-app-id", "test-hostname", egress.WithTCPDialer(mockDialer))
			Expect(err).ToNot(HaveOccurred())

			Expect(mockDialer.DialInput.Network).To(Receive(Equal("tcp")))
			Expect(mockDialer.DialInput.Address).To(Receive(Equal("example.com:1234")))
		})

		It("reconnects with a retry strategy", func() {
			env := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)
			mockDialer := newMockDialer()
			mockConn := newMockConn()
			mockRetry := newMockStrategy()

			By("Inital dial succeeds")
			mockDialer.DialOutput.Conn <- mockConn
			mockDialer.DialOutput.Err <- nil

			By("Failing to write to connection")
			close(mockConn.CloseOutput.Ret0)
			close(mockConn.WriteOutput.N)
			mockConn.WriteOutput.Err <- errors.New("write error")

			By("Failing to reconnect 3 times")
			for i := 0; i < 3; i++ {
				mockDialer.DialOutput.Err <- errors.New("dial err")
				mockDialer.DialOutput.Conn <- nil
			}

			By("Eventually succeeding to dial")
			mockDialer.DialOutput.Err <- nil
			mockDialer.DialOutput.Conn <- mockConn

			writer, err := egress.NewTCP(url.URL{
				Scheme: "syslog",
				Host:   "example.com:1234",
			}, "test-app-id", "test-hostname",
				egress.WithTCPDialer(mockDialer),
				egress.WithRetryStrategy(mockRetry.retry),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(mockDialer.DialCalled).To(Receive())

			By("Doing first write which fails and reconnects")
			writer.Write(env)
			Expect(mockDialer.DialCalled).To(Receive())
			Expect(mockRetry.callCount).To(Equal(uint64(3)))

			By("Doing second write which succeeds")
			close(mockConn.WriteOutput.Err)
			writer.Write(env)
			Expect(mockConn.WriteCalled).To(Receive())
		})
	})

	Describe("writing messages", func() {
		var (
			mockDrain *mockTCPDrain
			writer    *egress.TCPWriter
		)

		BeforeEach(func() {
			mockDrain = newMockTCPDrain()
			var err error
			writer, err = egress.NewTCP(url.URL{
				Scheme: "syslog",
				Host:   mockDrain.Addr().String(),
			}, "test-app-id", "test-hostname")
			Expect(err).ToNot(HaveOccurred())
		})

		DescribeTable("envelopes are written out with proper priority", func(logType loggregator_v2.Log_Type, expectedPriority int) {
			env := buildLogEnvelope("APP", "2", "just a test", logType)
			Expect(writer.Write(env)).To(Succeed())

			expectedOutput := fmt.Sprintf("92 <%d>1 1969-12-31T17:00:00.012345678-07:00 test-hostname test-app-id [APP/2] - - just a test\n", expectedPriority)
			Eventually(mockDrain.RXData).Should(Equal(expectedOutput))
		},
			Entry("stdout", loggregator_v2.Log_OUT, 14),
			Entry("stderr", loggregator_v2.Log_ERR, 11),
			Entry("undefined-type", loggregator_v2.Log_Type(-20), -1),
		)

		DescribeTable("envelopes are written out with proper process id", func(sourceType, sourceInstance, expectedProcessID string, expectedLength int) {
			env := buildLogEnvelope(sourceType, sourceInstance, "just a test", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(Succeed())

			expectedOutput := fmt.Sprintf("%d <14>1 1969-12-31T17:00:00.012345678-07:00 test-hostname test-app-id [%s] - - just a test\n", expectedLength, expectedProcessID)
			Eventually(mockDrain.RXData).Should(Equal(expectedOutput))
		},
			Entry("app source type", "app/foo/bar", "26", "APP/FOO/BAR/26", 101),
			Entry("other source type", "other", "1", "OTHER", 92),
		)

		It("strips null termination char from message", func() {
			env := buildLogEnvelope("OTHER", "1", "no null `\x00` please", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(Succeed())

			expectedOutput := fmt.Sprintf("98 <14>1 1969-12-31T17:00:00.012345678-07:00 test-hostname test-app-id [OTHER] - - no null `` please\n")
			Eventually(mockDrain.RXData).Should(Equal(expectedOutput))
		})

		It("ignores non-log envelopes", func() {
			env := buildCounterEnvelope()
			Expect(mockDrain.RXData()).To(BeEmpty())
			Expect(writer.Write(env)).To(Succeed())
			Expect(mockDrain.RXData()).To(BeEmpty())
		})
	})
})

func buildLogEnvelope(srcType, srcInstance, payload string, logType loggregator_v2.Log_Type) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		Tags: map[string]*loggregator_v2.Value{
			"source_type":     {&loggregator_v2.Value_Text{srcType}},
			"source_instance": {&loggregator_v2.Value_Text{srcInstance}},
		},
		Timestamp: 12345678,
		SourceId:  "source-id",
		Message: &loggregator_v2.Envelope_Log{
			Log: &loggregator_v2.Log{
				Payload: []byte(payload),
				Type:    logType,
			},
		},
	}
}

func buildCounterEnvelope() *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		Timestamp: 12345678,
		SourceId:  "source-id",
		Message: &loggregator_v2.Envelope_Counter{
			Counter: &loggregator_v2.Counter{
				Name: "some-counter",
			},
		},
	}
}

type mockTCPDrain struct {
	lis       net.Listener
	connCount uint64
	mu        sync.Mutex
	data      []byte
}

func newMockTCPDrain() *mockTCPDrain {
	lis, err := net.Listen("tcp", ":0")
	Expect(err).ToNot(HaveOccurred())
	m := &mockTCPDrain{
		lis: lis,
	}
	go m.accept()
	return m
}

func (m *mockTCPDrain) accept() {
	for {
		conn, err := m.lis.Accept()
		if err != nil {
			return
		}
		atomic.AddUint64(&m.connCount, 1)
		go m.handleConn(conn)
	}
}

func (m *mockTCPDrain) handleConn(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			log.Print(err)
			return
		}
		m.mu.Lock()
		m.data = append(m.data, buf[:n]...)
		m.mu.Unlock()
	}
}

func (m *mockTCPDrain) ConnCount() uint64 {
	return atomic.LoadUint64(&m.connCount)
}

func (m *mockTCPDrain) RXData() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return string(m.data)
}

func (m *mockTCPDrain) Addr() net.Addr {
	return m.lis.Addr()
}

type mockStrategy struct {
	callCount uint64
}

func newMockStrategy() *mockStrategy {
	return new(mockStrategy)
}

func (m *mockStrategy) retry(c int) time.Duration {
	m.callCount++
	return time.Nanosecond
}
