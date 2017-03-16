package ingress

// Binding reflects the JSON encoded output from the syslog drain binding provider
type Binding struct {
	AppID    string
	Hostname string
	Drain    string
}
type Bindings []Binding

func (b Bindings) DrainCount(search Binding) int {
	count := 0
	for _, binding := range b {
		if binding.AppID == search.AppID &&
			binding.Hostname == search.Hostname &&
			binding.Drain == search.Drain {
			count++
		}
	}
	return count
}
