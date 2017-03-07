package egress_test

import (
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("WriterBuilder", func() {
	var (
		builder    *egress.WriterBuilder
		binding    *v1.Binding
		mockDialer *mockDialer
	)

	BeforeEach(func() {
		binding = &v1.Binding{
			AppId:    "app-id",
			Hostname: "host-name",
		}
		mockDialer = newMockDialer()
		close(mockDialer.DialOutput.Conn)
		close(mockDialer.DialOutput.Err)

		builder = egress.NewWriterBuilder(
			egress.WithTCPOptions(
				egress.WithTCPDialer(mockDialer),
			),
		)
	})

	It("returns a tcp writer", func() {
		binding.Drain = "syslog://some-domain.tld"

		writer, err := builder.Build(binding)
		Expect(err).ToNot(HaveOccurred())

		Expect(writer).To(BeAssignableToTypeOf(&egress.TCPWriter{}))
	})

	It("returns an error for an unsupported syslog scheme", func() {
		binding.Drain = "bla://some-domain.tld"

		_, err := builder.Build(binding)
		Expect(err).To(MatchError("unsupported scheme"))
	})

	It("returns an error for an inproperly formatted drain", func() {
		binding.Drain = "://syslog/laksjdflk:asdfdsaf:2232"

		_, err := builder.Build(binding)
		Expect(err).To(HaveOccurred())
	})
})
