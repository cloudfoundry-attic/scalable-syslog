package egress_test

import (
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Binding List", func() {
	Context("DrainCount", func() {
		It("returns the number of times a drain appears", func() {
			firstBinding := &v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := &v1.Binding{"app-id", "org.space.app", "syslog://another-drain"}

			bindingList := egress.BindingList{
				{
					firstBinding,
				},
				{
					firstBinding,
					secondBinding,
				},
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(2))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})

		It("returns zero if the drain does not appear", func() {
			bindingList := egress.BindingList{}
			binding := &v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			Expect(bindingList.DrainCount(binding)).To(Equal(0))
		})

		It("separates out bindings with different app IDs", func() {
			firstBinding := &v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := &v1.Binding{"app-id-2", "org.space.app", "syslog://my-drain-url"}
			bindingList := egress.BindingList{
				{
					firstBinding,
					secondBinding,
				},
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})

		It("separates out bindings with different hostnames", func() {
			firstBinding := &v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := &v1.Binding{"app-id", "org.space.other-app", "syslog://my-drain-url"}
			bindingList := egress.BindingList{
				{
					firstBinding,
					secondBinding,
				},
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})

		It("separates out bindings with different drain URLs", func() {
			firstBinding := &v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}
			secondBinding := &v1.Binding{"app-id", "org.space.app", "syslog://my-other-drain-url"}
			bindingList := egress.BindingList{
				{
					firstBinding,
					secondBinding,
				},
			}
			Expect(bindingList.DrainCount(firstBinding)).To(Equal(1))
			Expect(bindingList.DrainCount(secondBinding)).To(Equal(1))
		})
	})
})
