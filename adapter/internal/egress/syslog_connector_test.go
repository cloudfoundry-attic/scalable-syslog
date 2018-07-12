package egress_test

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SyslogConnector", func() {
	var (
		ctx          context.Context
		spyWaitGroup *SpyWaitGroup
		netConf      egress.NetworkTimeoutConfig
	)

	BeforeEach(func() {
		ctx, _ = context.WithCancel(context.Background())
		spyWaitGroup = &SpyWaitGroup{}
	})

	It("connects to the passed syslog protocol", func() {
		var called bool
		constructor := func(
			*egress.URLBinding,
			egress.NetworkTimeoutConfig,
			bool,
			pulseemitter.CounterMetric,
		) egress.WriteCloser {
			called = true
			return &SleepWriterCloser{metric: nullMetric{}}
		}

		connector := egress.NewSyslogConnector(
			netConf,
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

	It("returns a writer that doesn't block even if the constructor's writer blocks", func() {
		slowConstructor := func(
			*egress.URLBinding,
			egress.NetworkTimeoutConfig,
			bool,
			pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return &SleepWriterCloser{
				metric:   nullMetric{},
				duration: time.Hour,
			}
		}

		connector := egress.NewSyslogConnector(
			netConf,
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
			netConf,
			true,
			spyWaitGroup,
		)

		binding := &v1.Binding{
			Drain: "bla://some-domain.tld",
		}
		_, err := connector.Connect(ctx, binding)
		Expect(err).To(MatchError("unsupported protocol"))
	})

	It("returns an error for an inproperly formatted drain", func() {
		connector := egress.NewSyslogConnector(
			netConf,
			true,
			spyWaitGroup,
		)

		binding := &v1.Binding{
			Drain: "://syslog/laksjdflk:asdfdsaf:2232",
		}

		_, err := connector.Connect(ctx, binding)
		Expect(err).To(HaveOccurred())
	})

	It("writes a LGR error for inproperly formatted drains", func() {
		logClient := newSpyLogClient()
		connector := egress.NewSyslogConnector(
			netConf,
			true,
			spyWaitGroup,
			egress.WithLogClient(logClient, "3"),
		)

		binding := &v1.Binding{
			AppId: "some-app-id",
			Drain: "://syslog/laksjdflk:asdfdsaf:2232",
		}

		_, _ = connector.Connect(ctx, binding)

		Expect(logClient.message()).To(ContainElement("Invalid syslog drain URL: parse failure"))
		Expect(logClient.appID()).To(ContainElement("some-app-id"))
		Expect(logClient.sourceType()).To(HaveKey("LGR"))
	})

	It("emits a metric when sending outbound messages", func() {
		writerConstructor := func(
			_ *egress.URLBinding,
			_ egress.NetworkTimeoutConfig,
			_ bool,
			m pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return &SleepWriterCloser{metric: m, duration: 0}
		}
		egressMetric := &testhelper.SpyMetric{}
		connector := egress.NewSyslogConnector(
			netConf,
			true,
			spyWaitGroup,
			egress.WithConstructors(map[string]egress.WriterConstructor{
				"protocol": writerConstructor,
			}),
			egress.WithEgressMetrics(map[string]pulseemitter.CounterMetric{
				"protocol": egressMetric,
			}),
		)

		binding := &v1.Binding{
			Drain: "protocol://",
		}
		writer, err := connector.Connect(ctx, binding)
		Expect(err).ToNot(HaveOccurred())

		for i := 0; i < 500; i++ {
			writer.Write(&loggregator_v2.Envelope{
				SourceId: "test-source-id",
			})
		}

		Eventually(egressMetric.Delta).Should(Equal(uint64(500)))
	})

	Describe("dropping messages", func() {
		var droppingConstructor = func(
			*egress.URLBinding,
			egress.NetworkTimeoutConfig,
			bool,
			pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return &SleepWriterCloser{
				metric:   nullMetric{},
				duration: time.Millisecond,
			}
		}

		It("emits a metric on dropped messages", func() {
			droppedMetric := &testhelper.SpyMetric{}

			connector := egress.NewSyslogConnector(
				netConf,
				true,
				spyWaitGroup,
				egress.WithConstructors(map[string]egress.WriterConstructor{
					"dropping": droppingConstructor,
				}),
				egress.WithDroppedMetrics(map[string]pulseemitter.CounterMetric{
					"dropping": droppedMetric,
				}),
			)

			binding := &v1.Binding{Drain: "dropping://"}

			writer, err := connector.Connect(ctx, binding)
			Expect(err).ToNot(HaveOccurred())

			go func(w egress.Writer) {
				for {
					w.Write(&loggregator_v2.Envelope{
						SourceId: "test-source-id",
					})
				}
			}(writer)

			Eventually(droppedMetric.Delta).Should(BeNumerically(">", 10000))
		})

		It("emits a LGR and SYS log to the log client about logs that have been dropped", func() {
			droppedMetric := &testhelper.SpyMetric{}
			binding := &v1.Binding{AppId: "app-id", Drain: "dropping://"}
			logClient := newSpyLogClient()

			connector := egress.NewSyslogConnector(
				netConf,
				true,
				spyWaitGroup,
				egress.WithConstructors(map[string]egress.WriterConstructor{
					"dropping": droppingConstructor,
				}),
				egress.WithDroppedMetrics(map[string]pulseemitter.CounterMetric{
					"dropping": droppedMetric,
				}),
				egress.WithLogClient(logClient, "3"),
			)

			writer, err := connector.Connect(ctx, binding)
			Expect(err).ToNot(HaveOccurred())

			go func(w egress.Writer) {
				for {
					w.Write(&loggregator_v2.Envelope{
						SourceId: "test-source-id",
					})
				}
			}(writer)

			Eventually(logClient.message).Should(ContainElement(MatchRegexp("\\d messages lost in user provided syslog drain")))
			Eventually(logClient.appID).Should(ContainElement("app-id"))

			Eventually(logClient.sourceType).Should(HaveLen(2))
			Eventually(logClient.sourceType).Should(HaveKey("LGR"))
			Eventually(logClient.sourceType).Should(HaveKey("SYS"))

			Eventually(logClient.sourceInstance).Should(HaveLen(2))
			Eventually(logClient.sourceInstance).Should(HaveKey(""))
			Eventually(logClient.sourceInstance).Should(HaveKey("3"))
		})

		It("does not panic on unknown dropped metrics", func() {
			binding := &v1.Binding{Drain: "dropping://"}

			connector := egress.NewSyslogConnector(
				netConf,
				true,
				spyWaitGroup,
				egress.WithConstructors(map[string]egress.WriterConstructor{
					"dropping": droppingConstructor,
				}),
				egress.WithDroppedMetrics(map[string]pulseemitter.CounterMetric{}),
			)

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
