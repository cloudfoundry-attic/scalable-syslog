package bindingmanager_test

import (
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/bindingmanager"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BindingManager", func() {
	var (
		manager *bindingmanager.BindingManager
	)

	BeforeEach(func() {
		manager = bindingmanager.New()
	})

	It("keeps track of the drains", func() {
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
		for i := 0; i < 2; i++ {
			manager.Add(&v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			})
		}

		bindings := manager.List()

		Expect(bindings).To(HaveLen(1))
	})

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
})
