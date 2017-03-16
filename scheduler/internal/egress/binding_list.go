package egress

import v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

type BindingList [][]*v1.Binding

func (b BindingList) DrainCount(search *v1.Binding) int {
	count := 0
	for _, bindings := range b {
		for _, binding := range bindings {
			if binding.AppId == search.AppId &&
				binding.Hostname == search.Hostname &&
				binding.Drain == search.Drain {
				count++
			}
		}
	}
	return count
}
