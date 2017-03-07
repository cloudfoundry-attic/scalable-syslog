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
			bindingList := egress.BindingList{
				{
					&v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"},
				},
				{
					&v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"},
					&v1.Binding{"app-id", "org.space.app", "syslog://another-drain"},
				},
			}
			Expect(bindingList.DrainCount("app-id", "syslog://my-drain-url")).To(Equal(2))
			Expect(bindingList.DrainCount("app-id", "syslog://another-drain")).To(Equal(1))
		})

		It("returns zero if the drain does not appear", func() {
			bindingList := egress.BindingList{}
			Expect(bindingList.DrainCount("app-id", "syslog://my-drain-url")).To(Equal(0))
		})
	})
})
