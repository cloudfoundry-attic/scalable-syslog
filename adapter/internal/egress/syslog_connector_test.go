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

	It("returns a writer that doesn't block even if the constructor's writer blocks", func(done Done) {
		defer close(done)
		blockedConstructor := func(*v1.Binding, time.Duration, time.Duration, bool) (egress.WriteCloser, error) {
			return &BlockedWriteCloser{}, nil
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			egress.WithConstructors(map[string]egress.SyslogConstructor{
				"blocked": blockedConstructor,
			}))

		binding := &v1.Binding{
			Drain: "blocked://",
		}
		writer, err := connector.Connect(binding)
		Expect(err).ToNot(HaveOccurred())
		err = writer.Write(&loggregator_v2.Envelope{
			SourceId: "test-source-id",
		})
		Expect(err).ToNot(HaveOccurred())
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

type BlockedWriteCloser struct {
	egress.WriteCloser
}

func (*BlockedWriteCloser) Write(*loggregator_v2.Envelope) error {
	for {
		time.Sleep(time.Second)
	}
}
