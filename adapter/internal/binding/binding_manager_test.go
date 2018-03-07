package binding_test

import (
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
	)

	BeforeEach(func() {
		metricClient = testhelper.NewMetricClient()
		subscriber = &SpySubscriber{}
		manager = binding.NewBindingManager(subscriber, metricClient)
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
			})

			It("does not return an error if binding already exists", func() {
				manager = binding.NewBindingManager(
					subscriber,
					metricClient,
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
