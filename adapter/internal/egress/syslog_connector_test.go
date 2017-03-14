package egress_test

import (
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SyslogConnector", func() {
	It("connects to the passed syslog scheme", func() {
		var called bool
		constructor := func(*v1.Binding, time.Duration, time.Duration, bool) (egress.WriteCloser, error) {
			called = true
			return nil, nil
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			egress.WithConstructors(map[string]egress.SyslogConstructor{
				"foo": constructor,
			}))

		binding := &v1.Binding{
			Drain: "foo://",
		}
		_, err := connector.Connect(binding)
		Expect(err).ToNot(HaveOccurred())
		Expect(called).To(BeTrue())
	})

	It("returns an error for an unsupported syslog scheme", func() {
		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
		)

		binding := &v1.Binding{
			Drain: "bla://some-domain.tld",
		}
		_, err := connector.Connect(binding)
		Expect(err).To(MatchError("unsupported scheme"))
	})

	It("returns an error for an inproperly formatted drain", func() {
		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
		)

		binding := &v1.Binding{
			Drain: "://syslog/laksjdflk:asdfdsaf:2232",
		}

		_, err := connector.Connect(binding)
		Expect(err).To(HaveOccurred())
	})
})
