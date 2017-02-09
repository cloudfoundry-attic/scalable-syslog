// Package orchestrator orchestrates CUPS bindings to adapters.
package orchestrator

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	adapterAddrs []string
}

// New creates an orchestrator.
func New(adapters []string) *Orchestrator {
	return &Orchestrator{
		adapterAddrs: adapters,
	}
}

// Count provides information on the configured adapters.
func (o *Orchestrator) Count() int {
	return len(o.adapterAddrs)
}
