package orchestrator

type Orchestrator struct {
	adapterAddrs []string
}

func New(adapters []string) *Orchestrator {
	return &Orchestrator{
		adapterAddrs: adapters,
	}
}

func (o *Orchestrator) Count() int {
	return len(o.adapterAddrs)
}
