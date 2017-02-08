package cupsprovider

import (
	"log"
	"time"
)

type Binding struct {
	Drains   []string
	Hostname string
}

type Provider interface {
	FetchBindings() (bindings map[string]Binding, err error)
}

type Store interface {
	StoreBindings(bindings map[string]Binding)
}

type poller struct {
	provider Provider
	store    Store
}

func StartPoller(interval time.Duration, provider Provider, store Store) {
	p := &poller{
		provider: provider,
		store:    store,
	}

	go p.run(interval)
}

func (p *poller) run(interval time.Duration) {
	for range time.Tick(interval) {
		bindings, err := p.provider.FetchBindings()
		if err != nil {
			log.Printf("Could not fetch bindings: %s", err)
			continue
		}
		p.store.StoreBindings(bindings)
	}
}
