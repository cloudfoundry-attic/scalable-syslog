package ingress_test

import (
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Binding List", func() {
	Context("DrainCount", func() {
		It("returns the number of times a drain appears", func() {
			firstBinding := ingress.Binding{AppID: "app-id", Hostname: "org.space.app", Drain: "syslog://my-drain-url"}
			secondBinding := ingress.Binding{AppID: "app-id", Hostname: "org.space.app", Drain: "syslog://another-drain"}

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
			binding := ingress.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			Expect(bindingList.DrainCount(binding)).To(Equal(0))
		})

		It("separates out bindings with different app IDs", func() {
			firstBinding := ingress.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := ingress.Binding{"app-id-2", "org.space.app", "syslog://my-drain-url"}
			bindingList := ingress.Bindings{
				firstBinding,
				secondBinding,
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})

		It("separates out bindings with different hostnames", func() {
			firstBinding := ingress.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := ingress.Binding{"app-id", "org.space.other-app", "syslog://my-drain-url"}
			bindingList := ingress.Bindings{
				firstBinding,
				secondBinding,
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})

		It("separates out bindings with different drain URLs", func() {
			firstBinding := ingress.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := ingress.Binding{"app-id", "org.space.app", "syslog://my-other-drain-url"}
			bindingList := ingress.Bindings{
				firstBinding,
				secondBinding,
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})
	})
})
