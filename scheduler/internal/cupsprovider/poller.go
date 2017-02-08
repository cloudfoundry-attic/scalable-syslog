// Package cupsprovider handles integration to the CUPS provider.
// It periodically reaches out to the provider and decodes its response.
// It then stores the result in a provided store.
package cupsprovider

import (
	"log"
	"time"
)

// Provider will fetch bindings from the CUPS provider
type Provider interface {
	FetchBindings() (bindings map[string]Binding, err error)
}

// Store will store the bindings
type Store interface {
	StoreBindings(bindings map[string]Binding)
}

type poller struct {
	provider Provider
	store    Store
}

// StartPoller starts polling the CUPS provider by invoking the Provider
// and storing the results in the Store
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
