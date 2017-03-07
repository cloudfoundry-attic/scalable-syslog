package egress

import v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

type BindingList [][]*v1.Binding

func (b BindingList) DrainCount(appID, drainURL string) int {
	count := 0
	for _, bindings := range b {
		for _, binding := range bindings {
			if binding.AppId == appID && binding.Drain == drainURL {
				count++
			}
		}
	}
	return count
}
