package egress_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/rfc5424"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("HTTPWriter", func() {
	It("errors when ssl validation is enabled", func() {
		drain := newMockOKDrain()

		b := buildURLBinding(drain.URL, "test-app-id", "test-hostname")

		writer := egress.NewHTTPSWriter(
			b,
			time.Second,
			time.Second,
			false,
			&testhelper.SpyMetric{},
		)

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		Expect(writer.Write(env)).To(HaveOccurred())
	})

	It("errors on an invalid syslog message", func() {
		drain := newMockOKDrain()

		b := buildURLBinding(
			drain.URL,
			"test-app-id-012345678901234567890012345678901234567890",
			"test-hostname",
		)
		writer := egress.NewHTTPSWriter(
			b,
			time.Second,
			time.Second,
			true,
			&testhelper.SpyMetric{},
		)

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		Expect(writer.Write(env)).To(HaveOccurred())
	})

	It("errors when the http POST fails", func() {
		drain := newMockErrorDrain()

		b := buildURLBinding(
			drain.URL,
			"test-app-id",
			"test-hostname",
		)

		writer := egress.NewHTTPSWriter(
			b,
			time.Second,
			time.Second,
			true,
			&testhelper.SpyMetric{},
		)

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		Expect(writer.Write(env)).To(HaveOccurred())
	})

	It("writes syslog formatted messages to http drain", func() {
		drain := newMockOKDrain()

		b := buildURLBinding(
			drain.URL,
			"test-app-id",
			"test-hostname",
		)

		writer := egress.NewHTTPSWriter(
			b,
			time.Second,
			time.Second,
			true,
			&testhelper.SpyMetric{},
		)

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
		metric := &testhelper.SpyMetric{}

		b := buildURLBinding(
			drain.URL,
			"test-app-id",
			"test-hostname",
		)

		writer := egress.NewHTTPSWriter(
			b,
			time.Second,
			time.Second,
			true,
			metric,
		)

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		writer.Write(env)

		Expect(metric.Delta()).To(Equal(uint64(1)))
	})

	It("ignores non-log envelopes", func() {
		drain := newMockOKDrain()

		b := buildURLBinding(
			drain.URL,
			"test-app-id",
			"test-hostname",
		)

		writer := egress.NewHTTPSWriter(
			b,
			time.Second,
			time.Second,
			true,
			&testhelper.SpyMetric{},
		)

		counterEnv := buildCounterEnvelope()
		logEnv := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)

		Expect(writer.Write(counterEnv)).To(Succeed())
		Expect(writer.Write(logEnv)).To(Succeed())
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

func buildURLBinding(u, appID, hostname string) *egress.URLBinding {
	parsedURL, _ := url.Parse(u)

	return &egress.URLBinding{
		URL:      parsedURL,
		AppID:    appID,
		Hostname: hostname,
	}
}
