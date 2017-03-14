package egress_test

import (
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SyslogConnector", func() {
	var (
		connector *egress.SyslogConnector
		binding   *v1.Binding
	)

	BeforeEach(func() {
		binding = &v1.Binding{
			AppId:    "app-id",
			Hostname: "host-name",
		}

		connector = egress.NewSyslogConnector(time.Second, time.Second, true)
	})

	It("returns an http writer", func() {
		binding.Drain = "https://some-fancy-uri"

		writer, err := connector.Connect(binding)
		Expect(err).ToNot(HaveOccurred())

		Expect(writer).To(BeAssignableToTypeOf(&egress.HTTPSWriter{}))
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

		Expect(writer).To(BeAssignableToTypeOf(&egress.TLSWriter{}))
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
})
