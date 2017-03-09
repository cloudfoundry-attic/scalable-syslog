package egress_test

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("TCPWriter", func() {
	Describe("NewTCPWriter()", func() {
		It("accepts a custom dialfunc", func() {
			var receivedAddr string
			dialFunc := func(addr string) (net.Conn, error) {
				receivedAddr = addr
				return &SpyConn{}, nil
			}

			_, err := egress.NewTCPWriter(
				&v1.Binding{
					AppId:    "test-app-id",
					Hostname: "test-hostname",
					Drain:    "syslog://example.com:1234",
				},
				time.Second,
				egress.WithDialFunc(dialFunc),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedAddr).To(Equal("example.com:1234"))
		})

		It("reconnects with a retry strategy", func() {
			env := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)

			spyConn := &SpyConn{}
			spyStrategy := SpyStrategy{}

			var callCount int
			dialFunc := func(addr string) (net.Conn, error) {
				callCount++

				if callCount > 1 && callCount < 5 {
					return nil, errors.New("dial err")
				}

				return spyConn, nil
			}

			writer, err := egress.NewTCPWriter(
				&v1.Binding{
					AppId:    "test-app-id",
					Hostname: "test-hostname",
					Drain:    "syslog://example.com:1234",
				},
				time.Second,
				egress.WithDialFunc(dialFunc),
				egress.WithRetryStrategy(spyStrategy.retry),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(callCount).To(Equal(1))

			By("Doing first write which fails and reconnects")
			spyConn.writeErr = errors.New("write error")
			err = writer.Write(env)
			Expect(err).To(HaveOccurred())
			Expect(callCount).To(Equal(5))
			Expect(spyStrategy.callCount).To(Equal(uint64(3)))

			By("Doing second write which succeeds")
			spyConn.writeErr = nil
			err = writer.Write(env)
			Expect(err).ToNot(HaveOccurred())
		})

		It("sets the write timeout", func() {
			env := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)
			spyConn := &SpyConn{}
			dialFunc := func(addr string) (net.Conn, error) {
				return spyConn, nil
			}

			writer, _ := egress.NewTCPWriter(
				&v1.Binding{
					AppId:    "test-app-id",
					Hostname: "test-hostname",
					Drain:    "syslog://example.com:1234",
				},
				time.Second,
				egress.WithDialFunc(dialFunc),
			)
			err := writer.Write(env)
			Expect(err).ToNot(HaveOccurred())
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
			dialFunc := func(addr string) (net.Conn, error) {
				return spyConn, nil
			}

			var err error
			writer, err = egress.NewTCPWriter(
				&v1.Binding{
					AppId:    "test-app-id",
					Hostname: "test-hostname",
					Drain:    "syslog://example.com:1234",
				},
				time.Second,
				egress.WithDialFunc(dialFunc))
			Expect(err).ToNot(HaveOccurred())
		})

		DescribeTable("envelopes are written out with proper priority", func(logType loggregator_v2.Log_Type, expectedPriority int) {
			env := buildLogEnvelope("APP", "2", "just a test", logType)
			Expect(writer.Write(env)).To(Succeed())

			expectedOutput := fmt.Sprintf("87 <%d>1 1970-01-01T00:00:00.012345678Z test-hostname test-app-id [APP/2] - - just a test\n", expectedPriority)
			Eventually(string(spyConn.writeInput)).Should(Equal(expectedOutput))
		},
			Entry("stdout", loggregator_v2.Log_OUT, 14),
			Entry("stderr", loggregator_v2.Log_ERR, 11),
			Entry("undefined-type", loggregator_v2.Log_Type(-20), -1),
		)

		DescribeTable("envelopes are written out with proper process id", func(sourceType, sourceInstance, expectedProcessID string, expectedLength int) {
			env := buildLogEnvelope(sourceType, sourceInstance, "just a test", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(Succeed())

			expectedOutput := fmt.Sprintf("%d <14>1 1970-01-01T00:00:00.012345678Z test-hostname test-app-id [%s] - - just a test\n", expectedLength, expectedProcessID)
			Eventually(string(spyConn.writeInput)).Should(Equal(expectedOutput))
		},
			Entry("app source type", "app/foo/bar", "26", "APP/FOO/BAR/26", 96),
			Entry("other source type", "other", "1", "OTHER", 87),
		)

		It("strips null termination char from message", func() {
			env := buildLogEnvelope("OTHER", "1", "no null `\x00` please", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(Succeed())

			expectedOutput := fmt.Sprintf("93 <14>1 1970-01-01T00:00:00.012345678Z test-hostname test-app-id [OTHER] - - no null `` please\n")
			Eventually(string(spyConn.writeInput)).Should(Equal(expectedOutput))
		})

		It("ignores non-log envelopes", func() {
			env := buildCounterEnvelope()
			Expect(writer.Write(env)).To(Succeed())
			Expect(spyConn.writeCalled).To(Equal(0))
		})
	})

	Describe("Close()", func() {
		It("closes the writer connection", func() {
			spyConn := &SpyConn{}
			dialFunc := func(addr string) (net.Conn, error) {
				return spyConn, nil
			}

			writer, err := egress.NewTCPWriter(
				&v1.Binding{
					AppId:    "test-app-id",
					Hostname: "test-hostname",
					Drain:    "syslog://example.com:1234",
				},
				time.Second,
				egress.WithDialFunc(dialFunc),
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(writer.Close()).To(Succeed())
			Expect(spyConn.closeCalled).To(Equal(1))
		})

		It("returns an error after writing to a closed connection", func() {
			spyConn := &SpyConn{}
			dialFunc := func(addr string) (net.Conn, error) {
				return spyConn, nil
			}

			writer, err := egress.NewTCPWriter(
				&v1.Binding{
					AppId:    "test-app-id",
					Hostname: "test-hostname",
					Drain:    "syslog://example.com:1234",
				},
				time.Second,
				egress.WithDialFunc(dialFunc),
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(writer.Close()).To(Succeed())

			env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(MatchError("connection does not exist"))
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
	callCount uint64
}

func (m *SpyStrategy) retry(c int) time.Duration {
	m.callCount++
	return time.Nanosecond
}

type SpyConn struct {
	net.Conn
	writeErr              error
	writeCalled           int
	writeInput            []byte
	setWriteDeadlineInput time.Time
	closeCalled           int
}

func (s *SpyConn) Write(b []byte) (n int, err error) {
	s.writeCalled++
	s.writeInput = b
	return 0, s.writeErr
}

func (s *SpyConn) Close() error {
	s.closeCalled++
	return nil
}

func (s *SpyConn) SetWriteDeadline(t time.Time) error {
	s.setWriteDeadlineInput = t
	return nil
}
