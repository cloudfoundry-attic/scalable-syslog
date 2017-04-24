package egress_test

import (
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/metric"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SyslogConnector", func() {
	var (
		metricEmitter *spyMetricEmitter
	)

	BeforeEach(func() {
		metricEmitter = newSpyMetricEmitter()
	})

	It("connects to the passed syslog scheme", func() {
		var called bool
		constructor := func(*v1.Binding, time.Duration, time.Duration, bool, egress.MetricEmitter) (egress.WriteCloser, error) {
			called = true
			return nil, nil
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			metricEmitter,
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
		blockedConstructor := func(*v1.Binding, time.Duration, time.Duration, bool, egress.MetricEmitter) (egress.WriteCloser, error) {
			return &BlockedWriteCloser{
				duration: time.Hour,
			}, nil
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			metricEmitter,
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
			metricEmitter,
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
			metricEmitter,
		)

		binding := &v1.Binding{
			Drain: "://syslog/laksjdflk:asdfdsaf:2232",
		}

		_, err := connector.Connect(binding)
		Expect(err).To(HaveOccurred())
	})

	It("emits a metric on dropped messages", func() {
		blockedConstructor := func(*v1.Binding, time.Duration, time.Duration, bool, egress.MetricEmitter) (egress.WriteCloser, error) {
			return &BlockedWriteCloser{
				duration: time.Millisecond,
			}, nil
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			metricEmitter,
			egress.WithConstructors(map[string]egress.SyslogConstructor{
				"blocked": blockedConstructor,
			}))

		binding := &v1.Binding{
			Drain: "blocked://",
		}
		writer, err := connector.Connect(binding)
		Expect(err).ToNot(HaveOccurred())

		go func(writer egress.WriteCloser) {
			for {
				writer.Write(&loggregator_v2.Envelope{
					SourceId: "test-source-id",
				})
			}
		}(writer)

		Eventually(metricEmitter.name).Should(Receive(Equal("dropped")))
		Expect(metricEmitter.opts).To(Receive(HaveLen(3)))
	})
})

type BlockedWriteCloser struct {
	duration time.Duration
	egress.WriteCloser
}

func (b *BlockedWriteCloser) Write(*loggregator_v2.Envelope) error {
	time.Sleep(b.duration)
	return nil
}

type spyMetricEmitter struct {
	name chan string
	opts chan []metric.IncrementOpt
}

func newSpyMetricEmitter() *spyMetricEmitter {
	return &spyMetricEmitter{
		name: make(chan string, 10),
		opts: make(chan []metric.IncrementOpt, 10),
	}
}

func (e *spyMetricEmitter) IncCounter(name string, options ...metric.IncrementOpt) {
	e.name <- name
	e.opts <- options
}
