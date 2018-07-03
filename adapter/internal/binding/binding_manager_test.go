package binding_test

import (
	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/binding"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BindingManager", func() {
	var (
		subscriber   *SpySubscriber
		manager      *binding.BindingManager
		metricClient *testhelper.SpyMetricClient
		logClient    *spyLogClient
	)

	BeforeEach(func() {
		metricClient = testhelper.NewMetricClient()
		subscriber = &SpySubscriber{}
		logClient = newSpyLogClient()
		manager = binding.NewBindingManager(subscriber, metricClient, logClient, "some-index")
	})

	Describe("Add()", func() {
		It("keeps track of the drains", func() {
			err := manager.Add(&v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			})
			Expect(err).ToNot(HaveOccurred())

			bindings := manager.List()

			Expect(bindings).To(HaveLen(1))
			Expect(bindings[0].AppId).To(Equal("some-id"))
			Expect(bindings[0].Hostname).To(Equal("some-hostname"))
			Expect(bindings[0].Drain).To(Equal("some.url"))
		})

		It("does not add duplicate bindings", func() {
			for i := 0; i < 2; i++ {
				err := manager.Add(&v1.Binding{
					AppId:    "some-id",
					Hostname: "some-hostname",
					Drain:    "some.url",
				})
				Expect(err).ToNot(HaveOccurred())
			}

			bindings := manager.List()

			Expect(bindings).To(HaveLen(1))
			Expect(subscriber.startCalled).To(Equal(1))
		})

		It("runs a subscription for the binding", func() {
			binding := &v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			}

			err := manager.Add(binding)
			Expect(err).ToNot(HaveOccurred())
			Expect(subscriber.start).To(Equal(binding))
		})

		Context("when maxBindings is exceeded", func() {
			It("returns an error and emits rejected_bindings metric", func() {
				manager = binding.NewBindingManager(
					subscriber,
					metricClient,
					logClient,
					"some-index",
					binding.WithMaxBindings(0),
				)

				binding := &v1.Binding{
					AppId:    "some-id",
					Hostname: "some-hostname",
					Drain:    "some.url",
				}

				err := manager.Add(binding)
				Expect(err).To(MatchError("Max bindings for adapter exceeded"))

				Expect(metricClient.GetMetric("rejected_bindings").Delta()).To(Equal(uint64(1)))
				Expect(logClient.envelopes).To(ContainElement(&loggregator_v2.Envelope{
					SourceId:   "some-id",
					InstanceId: "some-index",
					Message: &loggregator_v2.Envelope_Log{
						Log: &loggregator_v2.Log{
							Payload: []byte("Syslog adapter has failed to schedule your drain stream"),
							Type:    loggregator_v2.Log_ERR,
						},
					},
					Tags: map[string]string{
						"source_type": "LGR",
					},
				}))
			})

			It("does not return an error if binding already exists", func() {
				manager = binding.NewBindingManager(
					subscriber,
					metricClient,
					logClient,
					"some-index",
					binding.WithMaxBindings(1),
				)

				binding := &v1.Binding{
					AppId:    "some-id",
					Hostname: "some-hostname",
					Drain:    "some.url",
				}

				err := manager.Add(binding)
				Expect(err).ToNot(HaveOccurred())

				err = manager.Add(binding)
				Expect(err).ToNot(HaveOccurred())
			})

		})
	})

	Describe("Delete()", func() {
		It("removes a binding", func() {
			binding := &v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			}
			manager.Add(binding)
			manager.Delete(binding)

			Expect(manager.List()).To(HaveLen(0))
		})

		It("unsubscribes a binding", func() {
			binding := &v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			}
			manager.Add(binding)
			manager.Delete(binding)

			Expect(subscriber.stopCount).To(Equal(1))
		})
	})

	Describe("drain bindings metric", func() {
		It("increments and decrements as drains are added and removed", func() {
			bindingA := &v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			}
			bindingB := &v1.Binding{
				AppId:    "some-other-id",
				Hostname: "some-other-hostname",
				Drain:    "some.other-url",
			}

			manager.Add(bindingA)
			Expect(
				metricClient.GetMetric("drain_bindings").GaugeValue(),
			).To(Equal(float64(1)))

			manager.Add(bindingB)
			Expect(
				metricClient.GetMetric("drain_bindings").GaugeValue(),
			).To(Equal(float64(2)))

			manager.Delete(bindingA)
			Expect(
				metricClient.GetMetric("drain_bindings").GaugeValue(),
			).To(Equal(float64(1)))
		})
	})
})

type SpySubscriber struct {
	start       *v1.Binding
	startCalled int
	stopCount   int
}

func (s *SpySubscriber) Start(binding *v1.Binding) (stopFunc func()) {
	s.start = binding
	s.startCalled++

	return func() {
		s.stopCount++
	}
}

type spyLogClient struct {
	envelopes []*loggregator_v2.Envelope
}

func newSpyLogClient() *spyLogClient {
	return &spyLogClient{}
}

func (s *spyLogClient) EmitLog(message string, opts ...loggregator.EmitLogOption) {
	e := &loggregator_v2.Envelope{
		Message: &loggregator_v2.Envelope_Log{
			Log: &loggregator_v2.Log{
				Payload: []byte(message),
				Type:    loggregator_v2.Log_ERR,
			},
		},
		Tags: make(map[string]string),
	}

	for _, o := range opts {
		o(e)
	}

	s.envelopes = append(s.envelopes, e)
}
