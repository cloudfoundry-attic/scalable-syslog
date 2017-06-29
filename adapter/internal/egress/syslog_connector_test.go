package egress_test

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/pulseemitter/testhelper"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SyslogConnector", func() {
	var (
		metricEmitter *testhelper.SpyMetricClient
		ctx           context.Context
		cancelCtx     func()
		spyWaitGroup  *SpyWaitGroup
	)

	BeforeEach(func() {
		metricEmitter = testhelper.NewMetricClient()
		ctx, cancelCtx = context.WithCancel(context.Background())
		spyWaitGroup = &SpyWaitGroup{}
	})

	It("connects to the passed syslog scheme", func() {
		var called bool
		constructor := func(*v1.Binding, time.Duration, time.Duration, bool, *pulseemitter.CounterMetric) (egress.WriteCloser, error) {
			called = true
			return &BlockedWriteCloser{}, nil
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			metricEmitter,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.SyslogConstructor{
				"foo": constructor,
			}),
		)

		binding := &v1.Binding{
			Drain: "foo://",
		}
		_, err := connector.Connect(ctx, binding)
		Expect(err).ToNot(HaveOccurred())
		Expect(called).To(BeTrue())
	})

	It("returns a writer that doesn't block even if the constructor's writer blocks", func(done Done) {
		defer close(done)
		blockedConstructor := func(*v1.Binding, time.Duration, time.Duration, bool, *pulseemitter.CounterMetric) (egress.WriteCloser, error) {
			return &BlockedWriteCloser{
				duration: time.Hour,
			}, nil
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			metricEmitter,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.SyslogConstructor{
				"blocked": blockedConstructor,
			}),
		)

		binding := &v1.Binding{
			Drain: "blocked://",
		}
		writer, err := connector.Connect(ctx, binding)
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
			spyWaitGroup,
		)

		binding := &v1.Binding{
			Drain: "bla://some-domain.tld",
		}
		_, err := connector.Connect(ctx, binding)
		Expect(err).To(MatchError("unsupported scheme"))
	})

	It("returns an error for an inproperly formatted drain", func() {
		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			metricEmitter,
			spyWaitGroup,
		)

		binding := &v1.Binding{
			Drain: "://syslog/laksjdflk:asdfdsaf:2232",
		}

		_, err := connector.Connect(ctx, binding)
		Expect(err).To(HaveOccurred())
	})

	It("emits a metric on dropped messages", func() {
		blockedConstructor := func(*v1.Binding, time.Duration, time.Duration, bool, *pulseemitter.CounterMetric) (egress.WriteCloser, error) {
			return &BlockedWriteCloser{
				duration: time.Millisecond,
			}, nil
		}

		droppedMetric := &pulseemitter.CounterMetric{}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			metricEmitter,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.SyslogConstructor{
				"blocked": blockedConstructor,
			}),
			egress.WithDroppedMetrics(map[string]*pulseemitter.CounterMetric{
				"blocked": droppedMetric,
			}),
		)

		binding := &v1.Binding{
			Drain: "blocked://",
		}
		writer, err := connector.Connect(ctx, binding)
		Expect(err).ToNot(HaveOccurred())

		go func(writer egress.Writer) {
			for i := 0; i < 50000; i++ {
				writer.Write(&loggregator_v2.Envelope{
					SourceId: "test-source-id",
				})
			}
		}(writer)

		Eventually(func() uint64 {
			return droppedMetric.GetDelta()
		}).Should(BeNumerically(">", 10000))
	})

	It("does not panic on unknown dropped metrics", func() {
		unknownConstruct := func(*v1.Binding, time.Duration, time.Duration, bool, *pulseemitter.CounterMetric) (egress.WriteCloser, error) {
			return &BlockedWriteCloser{
				duration: time.Millisecond,
			}, nil
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			metricEmitter,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.SyslogConstructor{
				"unknown": unknownConstruct,
			}),
			egress.WithDroppedMetrics(map[string]*pulseemitter.CounterMetric{}),
		)

		binding := &v1.Binding{
			Drain: "unknown://",
		}
		writer, err := connector.Connect(ctx, binding)
		Expect(err).ToNot(HaveOccurred())

		f := func() {
			for i := 0; i < 50000; i++ {
				writer.Write(&loggregator_v2.Envelope{
					SourceId: "test-source-id",
				})
			}
		}
		Expect(f).ToNot(Panic())
	})
})

type BlockedWriteCloser struct {
	duration time.Duration
	io.Closer
}

func (b *BlockedWriteCloser) Write(*loggregator_v2.Envelope) error {
	time.Sleep(b.duration)
	return nil
}
