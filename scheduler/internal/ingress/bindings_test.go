package ingress_test

import (
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

var _ = Describe("Binding List", func() {
	Context("DrainCount", func() {
		It("returns the number of times a drain appears", func() {
			firstBinding := v1.Binding{AppId: "app-id", Hostname: "org.space.app", Drain: "syslog://my-drain-url"}
			secondBinding := v1.Binding{AppId: "app-id", Hostname: "org.space.app", Drain: "syslog://another-drain"}

			bindingList := ingress.Bindings{
				firstBinding,
				firstBinding,
				secondBinding,
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(2))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})

		It("returns zero if the drain does not appear", func() {
			bindingList := ingress.Bindings{}
			binding := v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			Expect(bindingList.DrainCount(binding)).To(Equal(0))
		})

		It("separates out bindings with different app IDs", func() {
			firstBinding := v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := v1.Binding{"app-id-2", "org.space.app", "syslog://my-drain-url"}
			bindingList := ingress.Bindings{
				firstBinding,
				secondBinding,
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})

		It("separates out bindings with different hostnames", func() {
			firstBinding := v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := v1.Binding{"app-id", "org.space.other-app", "syslog://my-drain-url"}
			bindingList := ingress.Bindings{
				firstBinding,
				secondBinding,
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})

		It("separates out bindings with different drain URLs", func() {
			firstBinding := v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := v1.Binding{"app-id", "org.space.app", "syslog://my-other-drain-url"}
			bindingList := ingress.Bindings{
				firstBinding,
				secondBinding,
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})
	})
})
