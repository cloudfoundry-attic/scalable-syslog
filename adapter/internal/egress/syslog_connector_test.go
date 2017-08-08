package egress_test

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SyslogConnector", func() {
	var (
		ctx          context.Context
		spyWaitGroup *SpyWaitGroup
	)

	BeforeEach(func() {
		ctx, _ = context.WithCancel(context.Background())
		spyWaitGroup = &SpyWaitGroup{}
	})

	It("connects to the passed syslog protocol", func() {
		var called bool
		constructor := func(
			*egress.URLBinding,
			time.Duration,
			time.Duration,
			bool,
			*pulseemitter.CounterMetric,
		) egress.WriteCloser {
			called = true
			return &SleepWriterCloser{metric: nullMetric{}}
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.WriterConstructor{
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
		slowConstructor := func(
			*egress.URLBinding,
			time.Duration,
			time.Duration,
			bool,
			*pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return &SleepWriterCloser{
				metric:   nullMetric{},
				duration: time.Hour,
			}
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.WriterConstructor{
				"slow": slowConstructor,
			}),
		)

		binding := &v1.Binding{
			Drain: "slow://",
		}
		writer, err := connector.Connect(ctx, binding)
		Expect(err).ToNot(HaveOccurred())
		err = writer.Write(&loggregator_v2.Envelope{
			SourceId: "test-source-id",
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns an error for an unsupported syslog protocol", func() {
		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
		)

		binding := &v1.Binding{
			Drain: "bla://some-domain.tld",
		}
		_, err := connector.Connect(ctx, binding)
		Expect(err).To(MatchError("unsupported protocol"))
	})

	It("writes an LGR error for an unsupported syslog protocol", func() {
		logClient := &spyLogClient{}
		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
			egress.WithLogClient(logClient),
		)

		binding := &v1.Binding{
			AppId: "some-app-id",
			Drain: "bla://some-domain.tld",
		}

		_, _ = connector.Connect(ctx, binding)

		Expect(logClient.calledWith).To(Equal("Invalid syslog drain URL: unsupported protocol"))
		Expect(logClient.appID).To(Equal("some-app-id"))
		Expect(logClient.sourceType).To(Equal("LGR"))
	})

	It("returns an error for an inproperly formatted drain", func() {
		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
		)

		binding := &v1.Binding{
			Drain: "://syslog/laksjdflk:asdfdsaf:2232",
		}

		_, err := connector.Connect(ctx, binding)
		Expect(err).To(HaveOccurred())
	})

	It("writes an LGR error for inproperly formatted drains", func() {
		logClient := &spyLogClient{}
		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
			egress.WithLogClient(logClient),
		)

		binding := &v1.Binding{
			AppId: "some-app-id",
			Drain: "://syslog/laksjdflk:asdfdsaf:2232",
		}

		_, _ = connector.Connect(ctx, binding)

		Expect(logClient.calledWith).To(Equal("Invalid syslog drain URL: parse failure"))
		Expect(logClient.appID).To(Equal("some-app-id"))
		Expect(logClient.sourceType).To(Equal("LGR"))
	})

	It("emits a metric when sending outbound messages", func() {
		writerConstructor := func(
			_ *egress.URLBinding,
			_ time.Duration,
			_ time.Duration,
			_ bool,
			m *pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return &SleepWriterCloser{metric: m, duration: 0}
		}
		egressMetric := &pulseemitter.CounterMetric{}
		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.WriterConstructor{
				"protocol": writerConstructor,
			}),
			egress.WithEgressMetrics(map[string]*pulseemitter.CounterMetric{
				"protocol": egressMetric,
			}),
		)

		binding := &v1.Binding{
			Drain: "protocol://",
		}
		writer, err := connector.Connect(ctx, binding)
		Expect(err).ToNot(HaveOccurred())

		go func(writer egress.Writer) {
			for i := 0; i < 500; i++ {
				writer.Write(&loggregator_v2.Envelope{
					SourceId: "test-source-id",
				})
			}
		}(writer)

		Eventually(func() int {
			return int(egressMetric.GetDelta())
		}).Should(Equal(500))
	})

	It("emits a metric on dropped messages", func() {
		droppingConstructor := func(
			*egress.URLBinding,
			time.Duration,
			time.Duration,
			bool,
			*pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return &SleepWriterCloser{
				metric:   nullMetric{},
				duration: time.Millisecond,
			}
		}

		droppedMetric := &pulseemitter.CounterMetric{}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.WriterConstructor{
				"dropping": droppingConstructor,
			}),
			egress.WithDroppedMetrics(map[string]*pulseemitter.CounterMetric{
				"dropping": droppedMetric,
			}),
		)

		binding := &v1.Binding{
			Drain: "dropping://",
		}
		writer, err := connector.Connect(ctx, binding)
		Expect(err).ToNot(HaveOccurred())

		go func(w egress.Writer) {
			for i := 0; i < 50000; i++ {
				w.Write(&loggregator_v2.Envelope{
					SourceId: "test-source-id",
				})
			}
		}(writer)

		Eventually(func() uint64 {
			return droppedMetric.GetDelta()
		}).Should(BeNumerically(">", 10000))
	})

	It("does not panic on unknown dropped metrics", func() {
		unknownConstructor := func(
			*egress.URLBinding,
			time.Duration,
			time.Duration,
			bool,
			*pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return &SleepWriterCloser{
				metric:   nullMetric{},
				duration: time.Millisecond,
			}
		}

		connector := egress.NewSyslogConnector(
			time.Second,
			time.Second,
			true,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.WriterConstructor{
				"unknown": unknownConstructor,
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

type incrementor interface {
	Increment(uint64)
}

type SleepWriterCloser struct {
	duration time.Duration
	io.Closer
	metric incrementor
}

func (c *SleepWriterCloser) Write(*loggregator_v2.Envelope) error {
	c.metric.Increment(1)
	time.Sleep(c.duration)
	return nil
}

type nullMetric struct{}

func (nullMetric) Increment(uint64) {}
