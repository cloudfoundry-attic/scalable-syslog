package egress_test

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("TCPWriter", func() {
	var binding = &v1.Binding{
		AppId:    "test-app-id",
		Hostname: "test-hostname",
		Drain:    "syslog://example.com:1234",
	}

	Describe("NewTCPWriter()", func() {
		It("dials up the drain", func() {
			spyDialer := SpyDialer{
				conn: &SpyConn{},
			}
			spyStrategy := SpyStrategy{}

			writer, err := egress.NewTCPWriter(
				binding,
				time.Second,
				egress.WithDialFunc(spyDialer.dialFunc),
				egress.WithRetryStrategy(spyStrategy.retry),
			)
			defer writer.Close()
			Expect(err).ToNot(HaveOccurred())
			Eventually(spyDialer.addr).Should(Equal("example.com:1234"))
		})

		It("reconnects with a retry strategy", func() {
			spyConn := &SpyConn{}
			spyDialer := SpyDialer{}
			spyStrategy := SpyStrategy{}
			env := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)

			By("connecting with a dialer that consistently returns an error")
			spyDialer.err = errors.New("test-error")
			writer, err := egress.NewTCPWriter(
				binding,
				time.Second,
				egress.WithDialFunc(spyDialer.dialFunc),
				egress.WithRetryStrategy(spyStrategy.retry),
			)
			defer writer.Close()
			Expect(err).ToNot(HaveOccurred())
			Eventually(spyDialer.called).Should(BeNumerically(">", 1))
			Eventually(spyStrategy.called).Should(BeNumerically(">", 1))

			By("connecting with a dialer that succeeds")
			spyDialer.setConn(spyConn)
			spyDialer.setErr(nil)
			Eventually(spyDialer.connected).Should(Equal(1))

			By("doing first write which fails and reconnects")
			spyConn.setWriteErr(errors.New("write error"))
			err = writer.Write(env)
			Expect(err).To(HaveOccurred())
			Eventually(spyDialer.connected).Should(Equal(2))

			By("doing second write which succeeds")
			spyConn.setWriteErr(nil)
			Eventually(func() error { return writer.Write(env) }).Should(Succeed())
		})

		It("sets the write timeout", func() {
			env := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)
			spyConn := &SpyConn{}
			spyDialer := &SpyDialer{
				conn: spyConn,
			}

			writer, _ := egress.NewTCPWriter(
				binding,
				time.Second,
				egress.WithDialFunc(spyDialer.dialFunc),
			)
			defer writer.Close()
			Eventually(spyDialer.connected).Should(Equal(1))
			Eventually(func() error { return writer.Write(env) }).Should(Succeed())
			Expect(spyConn.setWriteDeadlineInput).To(
				BeTemporally("~", time.Now().Add(time.Second), time.Millisecond*100),
			)
		})
	})

	Describe("Write()", func() {
		var (
			spyConn *SpyConn
			writer  *egress.TCPWriter
		)

		BeforeEach(func() {
			spyConn = &SpyConn{}
			spyDialer := &SpyDialer{
				conn: spyConn,
			}

			var err error
			writer, err = egress.NewTCPWriter(
				binding,
				time.Second,
				egress.WithDialFunc(spyDialer.dialFunc),
			)
			Expect(err).ToNot(HaveOccurred())
			Eventually(spyDialer.connected).Should(Equal(1))
		})

		AfterEach(func() {
			writer.Close()
		})

		DescribeTable("envelopes are written out with proper priority", func(logType loggregator_v2.Log_Type, expectedPriority int) {
			env := buildLogEnvelope("APP", "2", "just a test", logType)
			Eventually(func() error { return writer.Write(env) }).Should(Succeed())

			expectedOutput := fmt.Sprintf("87 <%d>1 1970-01-01T00:00:00.012345678Z test-hostname test-app-id [APP/2] - - just a test\n", expectedPriority)
			Expect(string(spyConn.writeInput)).To(Equal(expectedOutput))
		},
			Entry("stdout", loggregator_v2.Log_OUT, 14),
			Entry("stderr", loggregator_v2.Log_ERR, 11),
			Entry("undefined-type", loggregator_v2.Log_Type(-20), -1),
		)

		DescribeTable("envelopes are written out with proper process id", func(sourceType, sourceInstance, expectedProcessID string, expectedLength int) {
			env := buildLogEnvelope(sourceType, sourceInstance, "just a test", loggregator_v2.Log_OUT)
			Eventually(func() error { return writer.Write(env) }).Should(Succeed())

			expectedOutput := fmt.Sprintf("%d <14>1 1970-01-01T00:00:00.012345678Z test-hostname test-app-id [%s] - - just a test\n", expectedLength, expectedProcessID)
			Expect(string(spyConn.writeInput)).To(Equal(expectedOutput))
		},
			Entry("app source type", "app/foo/bar", "26", "APP/FOO/BAR/26", 96),
			Entry("other source type", "other", "1", "OTHER", 87),
		)

		It("strips null termination char from message", func() {
			env := buildLogEnvelope("OTHER", "1", "no null `\x00` please", loggregator_v2.Log_OUT)
			Eventually(func() error { return writer.Write(env) }).Should(Succeed())

			expectedOutput := fmt.Sprintf("93 <14>1 1970-01-01T00:00:00.012345678Z test-hostname test-app-id [OTHER] - - no null `` please\n")
			Expect(string(spyConn.writeInput)).To(Equal(expectedOutput))
		})

		It("ignores non-log envelopes", func() {
			env := buildCounterEnvelope()
			Expect(writer.Write(env)).To(Succeed())
			Expect(spyConn.writeCalled).To(Equal(0))
		})
	})

	Describe("Close()", func() {
		Context("with a happy dialer", func() {
			var (
				spyConn *SpyConn
				writer  *egress.TCPWriter
			)

			BeforeEach(func() {
				spyConn = &SpyConn{}
				spyDialer := &SpyDialer{
					conn: spyConn,
				}

				var err error
				writer, err = egress.NewTCPWriter(
					binding,
					time.Second,
					egress.WithDialFunc(spyDialer.dialFunc),
				)
				Expect(err).ToNot(HaveOccurred())
				Eventually(spyDialer.connected).Should(Equal(1))

				env := buildLogEnvelope("", "", "just a test", loggregator_v2.Log_OUT)
				Eventually(func() error { return writer.Write(env) }).Should(Succeed())
			})

			It("closes the writer connection", func() {
				Expect(writer.Close()).To(Succeed())
				Expect(spyConn.closeCalled()).To(Equal(1))
			})

			It("returns an error after writing to a closed connection", func() {
				Expect(writer.Close()).To(Succeed())

				env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
				Expect(writer.Write(env)).To(MatchError("connection does not exist"))
			})
		})

		Context("with a dialer that is erroring", func() {
			It("stops connecting", func() {
				spyDialer := &SpyDialer{
					err: errors.New("test-error"),
				}

				writer, _ := egress.NewTCPWriter(
					binding,
					time.Second,
					egress.WithDialFunc(spyDialer.dialFunc),
				)
				Eventually(spyDialer.called).Should(BeNumerically(">", 2))

				Expect(writer.Close()).To(Succeed())
				called := spyDialer.called()
				Consistently(spyDialer.called).Should(BeNumerically("~", called, 1))
			})
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

type SpyStrategy struct {
	mu      sync.Mutex
	called_ uint64
}

func (m *SpyStrategy) retry(c int) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.called_++
	return time.Millisecond
}

func (m *SpyStrategy) called() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.called_
}

type SpyConn struct {
	net.Conn
	mu                    sync.Mutex
	writeErr              error
	writeCalled           int
	writeInput            []byte
	setWriteDeadlineInput time.Time
	closeCalled_          int
}

func (s *SpyConn) Write(b []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.writeCalled++
	s.writeInput = b
	return 0, s.writeErr
}

func (s *SpyConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closeCalled_++
	return nil
}

func (s *SpyConn) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.setWriteDeadlineInput = t
	return nil
}

func (s *SpyConn) closeCalled() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeCalled_
}

func (s *SpyConn) setWriteErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeErr = err
}

type SpyDialer struct {
	mu         sync.Mutex
	conn       net.Conn
	err        error
	addr_      string
	called_    int
	connected_ int
}

func (s *SpyDialer) dialFunc(addr string) (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.called_++
	s.addr_ = addr
	if s.err == nil {
		s.connected_++
	}
	return s.conn, s.err
}

func (s *SpyDialer) called() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.called_
}

func (s *SpyDialer) connected() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected_
}

func (s *SpyDialer) setConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = conn
}

func (s *SpyDialer) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *SpyDialer) addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr_
}
