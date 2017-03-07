package bindingmanager_test

import (
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/bindingmanager"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BindingManager", func() {
	var (
		subscriber *mockSubscriber
		manager    *bindingmanager.BindingManager
	)

	BeforeEach(func() {
		subscriber = newMockSubscriber()
		manager = bindingmanager.New(subscriber)
	})

	Describe("Add()", func() {
		It("keeps track of the drains", func() {
			close(subscriber.StartOutput.StopFunc)
			manager.Add(&v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			})

			bindings := manager.List()

			Expect(bindings).To(HaveLen(1))
			Expect(bindings[0].AppId).To(Equal("some-id"))
			Expect(bindings[0].Hostname).To(Equal("some-hostname"))
			Expect(bindings[0].Drain).To(Equal("some.url"))
		})

		It("does not add duplicate bindings", func() {
			close(subscriber.StartOutput.StopFunc)
			for i := 0; i < 2; i++ {
				manager.Add(&v1.Binding{
					AppId:    "some-id",
					Hostname: "some-hostname",
					Drain:    "some.url",
				})
			}

			bindings := manager.List()

			Expect(bindings).To(HaveLen(1))
			Expect(subscriber.StartCalled).To(HaveLen(1))
		})

		It("runs a subscription for the binding", func() {
			binding := &v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			}
			close(subscriber.StartOutput.StopFunc)

			manager.Add(binding)
			Expect(subscriber.StartInput.Binding).To(Receive(Equal(binding)))
		})
	})

	Describe("Delete()", func() {
		It("removes a binding", func() {
			var stopCount int
			subscriber.StartOutput.StopFunc <- func() { stopCount++ }

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
			var stopCount int
			subscriber.StartOutput.StopFunc <- func() { stopCount++ }

			binding := &v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			}
			manager.Add(binding)
			manager.Delete(binding)

			Expect(stopCount).To(Equal(1))
		})
	})
})
