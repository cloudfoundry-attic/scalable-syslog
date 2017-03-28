package egress_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/crewjam/rfc5424"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("HTTPWriter", func() {
	It("does not accept schemes other than http", func() {
		b := &v1.Binding{
			Drain: "syslog://example.com:1123",
		}
		_, err := egress.NewHTTPSWriter(b, time.Second, time.Second, true)
		Expect(err).To(HaveOccurred())
	})

	It("errors when ssl validation is enabled", func() {
		drain := newMockOKDrain()

		b := &v1.Binding{
			Drain:    drain.URL,
			AppId:    "test-app-id",
			Hostname: "test-hostname",
		}
		writer, err := egress.NewHTTPSWriter(b, time.Second, time.Second, false)
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
		writer, err := egress.NewHTTPSWriter(b, time.Second, time.Second, true)
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

		writer, err := egress.NewHTTPSWriter(b, time.Second, time.Second, true)
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
		writer, err := egress.NewHTTPSWriter(b, time.Second, time.Second, true)
		Expect(err).ToNot(HaveOccurred())

		env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
		Expect(writer.Write(env)).To(Succeed())

		Expect(drain.messages).To(HaveLen(1))
		Expect(drain.messages[0].AppName).To(Equal("test-app-id"))
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
