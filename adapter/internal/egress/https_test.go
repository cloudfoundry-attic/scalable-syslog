package egress_test

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/crewjam/rfc5424"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("HTTPWriter", func() {
	It("does not accept schemes other than http", func() {
		b := &v1.Binding{
			Drain: "syslog://example.com:1123",
		}
		_, err := egress.NewHTTPSWriter(
			context.TODO(),
			b,
			time.Second,
			time.Second,
			true,
			new(pulseemitter.CounterMetric),
		)
		Expect(err).To(HaveOccurred())
	})

	It("errors when ssl validation is enabled", func() {
		drain := newMockOKDrain()

		b := &v1.Binding{
			Drain:    drain.URL,
			AppId:    "test-app-id",
			Hostname: "test-hostname",
		}
		writer, err := egress.NewHTTPSWriter(
			context.TODO(),
			b,
			time.Second,
			time.Second,
			false,
			new(pulseemitter.CounterMetric),
		)
		Expect(err).ToNot(HaveOccurred())

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		Expect(writer.Write(env)).To(HaveOccurred())
	})

	It("errors on an invalid syslog message", func() {
		drain := newMockOKDrain()

		b := &v1.Binding{
			Drain:    drain.URL,
			AppId:    "test-app-id-012345678901234567890012345678901234567890",
			Hostname: "test-hostname",
		}
		writer, err := egress.NewHTTPSWriter(
			context.TODO(),
			b,
			time.Second,
			time.Second,
			true,
			new(pulseemitter.CounterMetric),
		)
		Expect(err).ToNot(HaveOccurred())

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		Expect(writer.Write(env)).To(HaveOccurred())
	})

	It("errors when the http POST fails", func() {
		drain := newMockErrorDrain()

		b := &v1.Binding{
			Drain:    drain.URL,
			AppId:    "test-app-id",
			Hostname: "test-hostname",
		}

		writer, err := egress.NewHTTPSWriter(
			context.TODO(),
			b,
			time.Second,
			time.Second,
			true,
			new(pulseemitter.CounterMetric),
		)
		Expect(err).ToNot(HaveOccurred())

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		Expect(writer.Write(env)).To(HaveOccurred())
	})

	It("writes syslog formatted messages to http drain", func() {
		drain := newMockOKDrain()

		b := &v1.Binding{
			Drain:    drain.URL,
			AppId:    "test-app-id",
			Hostname: "test-hostname",
		}
		writer, err := egress.NewHTTPSWriter(
			context.TODO(),
			b,
			time.Second,
			time.Second,
			true,
			new(pulseemitter.CounterMetric),
		)
		Expect(err).ToNot(HaveOccurred())

		env1 := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		Expect(writer.Write(env1)).To(Succeed())
		env2 := buildLogEnvelope("CELL", "5", "log from cell", loggregator_v2.Log_ERR)
		Expect(writer.Write(env2)).To(Succeed())
		env3 := buildLogEnvelope("CELL", "", "log from cell", loggregator_v2.Log_ERR)
		Expect(writer.Write(env3)).To(Succeed())

		Expect(drain.messages).To(HaveLen(3))
		expected := &rfc5424.Message{
			AppName:   "test-app-id",
			Hostname:  "test-hostname",
			Priority:  rfc5424.Priority(14),
			ProcessID: "[APP/1]",
			Message:   []byte("just a test\n"),
		}
		Expect(drain.messages[0].AppName).To(Equal(expected.AppName))
		Expect(drain.messages[0].Hostname).To(Equal(expected.Hostname))
		Expect(drain.messages[0].Priority).To(BeEquivalentTo(expected.Priority))
		Expect(drain.messages[0].ProcessID).To(Equal(expected.ProcessID))
		Expect(drain.messages[0].Message).To(Equal(expected.Message))

		expected = &rfc5424.Message{
			AppName:   "test-app-id",
			Hostname:  "test-hostname",
			Priority:  rfc5424.Priority(11),
			ProcessID: "[CELL/5]",
			Message:   []byte("log from cell\n"),
		}
		Expect(drain.messages[1].AppName).To(Equal(expected.AppName))
		Expect(drain.messages[1].Hostname).To(Equal(expected.Hostname))
		Expect(drain.messages[1].Priority).To(BeEquivalentTo(expected.Priority))
		Expect(drain.messages[1].ProcessID).To(Equal(expected.ProcessID))
		Expect(drain.messages[1].Message).To(Equal(expected.Message))

		Expect(drain.messages[2].ProcessID).To(Equal("[CELL]"))
	})

	It("emits an egress metric for each message", func() {
		drain := newMockOKDrain()
		metric := new(pulseemitter.CounterMetric)

		b := &v1.Binding{
			Drain:    drain.URL,
			AppId:    "test-app-id",
			Hostname: "test-hostname",
		}
		writer, err := egress.NewHTTPSWriter(
			context.TODO(),
			b,
			time.Second,
			time.Second,
			true,
			metric,
		)
		Expect(err).ToNot(HaveOccurred())

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		writer.Write(env)

		Expect(metric.GetDelta()).To(Equal(uint64(1)))
	})
})

type SpyDrain struct {
	*httptest.Server
	messages []*rfc5424.Message
}

func newMockOKDrain() *SpyDrain {
	return newMockDrain(http.StatusOK)
}

func newMockErrorDrain() *SpyDrain {
	return newMockDrain(http.StatusBadRequest)
}

func newMockDrain(status int) *SpyDrain {
	drain := &SpyDrain{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		message := &rfc5424.Message{}

		body, err := ioutil.ReadAll(r.Body)
		Expect(err).ToNot(HaveOccurred())
		defer r.Body.Close()

		err = message.UnmarshalBinary(body)
		Expect(err).ToNot(HaveOccurred())

		drain.messages = append(drain.messages, message)
		w.WriteHeader(status)
	})
	server := httptest.NewTLSServer(handler)
	drain.Server = server
	return drain
}
