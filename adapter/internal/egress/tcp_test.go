package egress_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/url"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("TCPWriter", func() {
	var (
		listener net.Listener
		binding  = &egress.URLBinding{
			AppID:    "test-app-id",
			Hostname: "test-hostname",
		}
	)

	BeforeEach(func() {
		var err error
		listener, err = net.Listen("tcp", ":0")
		Expect(err).ToNot(HaveOccurred())
		binding.URL, _ = url.Parse(fmt.Sprintf("syslog://%s", listener.Addr()))
	})

	AfterEach(func() {
		listener.Close()
	})

	Describe("Write()", func() {
		var (
			writer        egress.WriteCloser
			egressCounter *testhelper.SpyMetric
		)

		BeforeEach(func() {
			var err error
			egressCounter = &testhelper.SpyMetric{}

			writer = egress.NewTCPWriter(
				binding,
				time.Second,
				time.Second,
				false,
				egressCounter,
			)
			Expect(err).ToNot(HaveOccurred())
		})

		DescribeTable("envelopes are written out with proper priority", func(logType loggregator_v2.Log_Type, expectedPriority int) {
			env := buildLogEnvelope("APP", "2", "just a test", logType)
			Expect(writer.Write(env)).To(Succeed())

			conn, err := listener.Accept()
			Expect(err).ToNot(HaveOccurred())
			buf := bufio.NewReader(conn)

			actual, err := buf.ReadString('\n')
			Expect(err).ToNot(HaveOccurred())

			expected := fmt.Sprintf("89 <%d>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [APP/2] - - just a test\n", expectedPriority)
			Expect(actual).To(Equal(expected))
		},
			Entry("stdout", loggregator_v2.Log_OUT, 14),
			Entry("stderr", loggregator_v2.Log_ERR, 11),
			Entry("undefined-type", loggregator_v2.Log_Type(-20), -1),
		)

		DescribeTable("envelopes are written out with proper process id", func(sourceType, sourceInstance, expectedProcessID string, expectedLength int) {
			env := buildLogEnvelope(sourceType, sourceInstance, "just a test", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(Succeed())

			conn, err := listener.Accept()
			Expect(err).ToNot(HaveOccurred())
			buf := bufio.NewReader(conn)

			actual, err := buf.ReadString('\n')
			Expect(err).ToNot(HaveOccurred())

			expected := fmt.Sprintf("%d <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [%s] - - just a test\n", expectedLength, expectedProcessID)
			Expect(actual).To(Equal(expected))
		},
			Entry("app source type", "app/foo/bar", "26", "APP/FOO/BAR/26", 98),
			Entry("other source type", "other", "1", "OTHER/1", 91),
		)

		It("strips null termination char from message", func() {
			env := buildLogEnvelope("OTHER", "1", "no null `\x00` please", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(Succeed())

			conn, err := listener.Accept()
			Expect(err).ToNot(HaveOccurred())
			buf := bufio.NewReader(conn)

			actual, err := buf.ReadString('\n')
			Expect(err).ToNot(HaveOccurred())

			expected := fmt.Sprintf("97 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [OTHER/1] - - no null `` please\n")
			Expect(actual).To(Equal(expected))
		})

		It("ignores non-log envelopes", func() {
			counterEnv := buildCounterEnvelope()
			logEnv := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)

			Expect(writer.Write(counterEnv)).To(Succeed())
			Expect(writer.Write(logEnv)).To(Succeed())

			conn, err := listener.Accept()
			Expect(err).ToNot(HaveOccurred())
			buf := bufio.NewReader(conn)

			actual, err := buf.ReadString('\n')
			Expect(err).ToNot(HaveOccurred())

			expected := "89 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [APP/2] - - just a test\n"
			Expect(actual).To(Equal(expected))
		})

		It("emits an egress metric for each message", func() {
			env := buildLogEnvelope("OTHER", "1", "no null `\x00` please", loggregator_v2.Log_OUT)
			writer.Write(env)

			Expect(egressCounter.Delta()).To(Equal(uint64(1)))
		})

		It("replaces spaces with dashes in the process ID", func() {
			env := buildLogEnvelope("MY TASK", "2", "just a test", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(Succeed())

			conn, err := listener.Accept()
			Expect(err).ToNot(HaveOccurred())
			buf := bufio.NewReader(conn)

			actual, err := buf.ReadString('\n')
			Expect(err).ToNot(HaveOccurred())

			Expect(actual).To(Equal(
				"93 <14>1 1970-01-01T00:00:00.012345+00:00 test-hostname test-app-id [MY-TASK/2] - - just a test\n",
			))
		})
	})

	Describe("when write fails to connect", func() {
		It("write returns an error", func() {
			env := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)
			binding.URL, _ = url.Parse("syslog://localhost-garbage:9999")

			writer := egress.NewTCPWriter(
				binding,
				time.Second,
				time.Second,
				false,
				&testhelper.SpyMetric{},
			)

			errs := make(chan error, 1)
			go func() {
				errs <- writer.Write(env)
			}()
			Eventually(errs, 3).Should(Receive(HaveOccurred()))
		})
	})

	Describe("Cancel Context", func() {
		var (
			writer egress.WriteCloser
			conn   net.Conn
		)

		Context("with a happy dialer", func() {
			BeforeEach(func() {
				var err error
				writer = egress.NewTCPWriter(
					binding,
					time.Second,
					time.Second,
					false,
					&testhelper.SpyMetric{},
				)
				Expect(err).ToNot(HaveOccurred())

				By("writing to establish connection")
				logEnv := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)
				err = writer.Write(logEnv)
				Expect(err).ToNot(HaveOccurred())

				conn, err = listener.Accept()
				Expect(err).ToNot(HaveOccurred())

				b := make([]byte, 256)
				_, err = conn.Read(b)
				Expect(err).ToNot(HaveOccurred())
			})

			It("closes the writer connection", func() {
				Expect(writer.Close()).To(Succeed())

				b := make([]byte, 256)
				_, err := conn.Read(b)
				Expect(err).To(Equal(io.EOF))
			})
		})
	})
})

func buildLogEnvelope(srcType, srcInstance, payload string, logType loggregator_v2.Log_Type) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		Tags: map[string]string{
			"source_type": srcType,
		},
		InstanceId: srcInstance,
		Timestamp:  12345678,
		SourceId:   "source-id",
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
