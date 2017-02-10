// Package cups handles integration to the CUPS provider.
// It periodically reaches out to the provider and decodes its response.
// It then stores the result in a provided store.
package app

import (
	"log"
	"time"
)

// Provider will fetch bindings from the CUPS provider
type Provider interface {
	FetchBindings() (bindings AppBindings, err error)
}

// Store will store the bindings
type Store interface {
	StoreBindings(AppBindings)
}

type poller struct {
	provider Provider
}

// StartPoller starts polling the CUPS provider by invoking the Provider
// and storing the results in the Store
func StartPoller(interval time.Duration, provider Provider) {
	p := &poller{
		provider: provider,
	}

	go p.run(interval)
}

func (p *poller) run(interval time.Duration) {
	for range time.Tick(interval) {
		_, err := p.provider.FetchBindings()
		if err != nil {
			log.Printf("Could not fetch bindings: %s", err)
			continue
		}
	}
}
