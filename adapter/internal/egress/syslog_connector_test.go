package egress_test

import (
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SyslogConnector", func() {
	var (
		connector            *egress.SyslogConnector
		binding              *v1.Binding
		oldWriterConstructor func(*v1.Binding, time.Duration, ...egress.TCPOption) (*egress.TCPWriter, error)
	)

	BeforeEach(func() {
		binding = &v1.Binding{
			AppId:    "app-id",
			Hostname: "host-name",
		}

		oldWriterConstructor = egress.NewTCPWriter
		egress.NewTCPWriter = func(*v1.Binding, time.Duration, ...egress.TCPOption) (*egress.TCPWriter, error) {
			return nil, nil
		}
		connector = egress.NewSyslogConnector(time.Second, time.Second, true)
	})

	AfterEach(func() {
		egress.NewTCPWriter = oldWriterConstructor
	})

	It("returns a tcp writer", func() {
		binding.Drain = "syslog://some-domain.tld"

		writer, err := connector.Connect(binding)
		Expect(err).ToNot(HaveOccurred())

		Expect(writer).To(BeAssignableToTypeOf(&egress.TCPWriter{}))
	})

	It("returns a tls writer", func() {
		binding.Drain = "syslog-tls://some-domain.tld"

		writer, err := connector.Connect(binding)
		Expect(err).ToNot(HaveOccurred())

		Expect(writer).To(BeAssignableToTypeOf(&egress.TCPWriter{}))
	})

	It("returns an error for an unsupported syslog scheme", func() {
		binding.Drain = "bla://some-domain.tld"

		_, err := connector.Connect(binding)
		Expect(err).To(MatchError("unsupported scheme"))
	})

	It("returns an error for an inproperly formatted drain", func() {
		binding.Drain = "://syslog/laksjdflk:asdfdsaf:2232"

		_, err := connector.Connect(binding)
		Expect(err).To(HaveOccurred())
	})

	Context("when passed an https scheme", func() {
		It("returns an http writer", func() {
			binding.Drain = "https://some-fancy-uri"

			writer, err := connector.Connect(binding)
			Expect(err).ToNot(HaveOccurred())

			Expect(writer).To(BeAssignableToTypeOf(&egress.HTTPSWriter{}))
		})

		It("get's passed the value from the connector", func() {
			connector := egress.NewSyslogConnector(time.Second, time.Second, false)
			drain := newMockOKDrain()

			b := &v1.Binding{
				Drain:    drain.URL,
				AppId:    "test-app-id",
				Hostname: "test-hostname",
			}
			writer, err := connector.Connect(b)
			Expect(err).ToNot(HaveOccurred())

			env := buildLogEnvelope("APP", "1", "just a test", loggregator_v2.Log_OUT)
			Expect(writer.Write(env)).To(HaveOccurred())
		})
	})
})
