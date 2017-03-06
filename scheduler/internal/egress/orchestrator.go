// Package orchestrator orchestrates CUPS bindings to adapters.
package egress

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
)

type BindingReader interface {
	FetchBindings() (appBindings ingress.AppBindings, err error)
}

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	reader BindingReader
	pool   AdapterPool
	once   sync.Once
	done   chan bool
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(r BindingReader, p AdapterPool) *Orchestrator {
	return &Orchestrator{
		reader: r,
		pool:   p,
		done:   make(chan bool),
	}
}

// Run starts the orchestrator.
func (o *Orchestrator) Run(interval time.Duration) {
	for {
		select {
		case <-time.Tick(interval):
			expected, err := o.reader.FetchBindings()
			if err != nil {
				continue
			}

			actual, err := o.List()
			if err != nil {
				log.Printf("Failed to get actual bindings: %s", err)
				continue
			}

			o.DeleteAll(actual, expected)
			o.Create(expected)
		case <-o.done:
			return
		}
	}
}

func (o *Orchestrator) Stop() {
	o.once.Do(func() {
		o.done <- true
	})
}

func (o *Orchestrator) Create(expected ingress.AppBindings) {
	for appID, cupsBinding := range expected {
		for _, drain := range cupsBinding.Drains {
			b := &v1.Binding{
				Hostname: cupsBinding.Hostname,
				AppId:    appID,
				Drain:    drain,
			}
			request := &v1.CreateBindingRequest{
				Binding: b,
			}

			clientLen := len(o.pool)
			switch clientLen {
			case 1:
				client := o.pool[0]
				client.CreateBinding(context.Background(), request)
			case 2:
				for _, client := range o.pool {
					client.CreateBinding(context.Background(), request)
				}
			default:
				c1Index := rand.Intn(clientLen)
				c2Index := rand.Intn(clientLen)
				c1 := o.pool[c1Index]
				c2 := o.pool[c2Index]

				c1.CreateBinding(context.Background(), request)
				c2.CreateBinding(context.Background(), request)
			}
		}
	}
}

func (o *Orchestrator) DeleteAll(actual [][]*v1.Binding, expected ingress.AppBindings) {
	var toDelete []*v1.Binding
	for _, adapterBindings := range actual {
		for _, ab := range adapterBindings {
			if !exists(expected, ab) {
				toDelete = append(toDelete, ab)
			}
		}
	}

	for _, ab := range toDelete {
		request := &v1.DeleteBindingRequest{
			Binding: ab,
		}

		for _, client := range o.pool {
			client.DeleteBinding(context.Background(), request)
		}
	}
}

func exists(actual ingress.AppBindings, ab *v1.Binding) bool {
	b, ok := actual[ab.AppId]
	if !ok {
		return false
	}

	for _, d := range b.Drains {
		if d == ab.Drain {
			return true
		}
	}

	return false
}

func (o *Orchestrator) List() ([][]*v1.Binding, error) {
	request := new(v1.ListBindingsRequest)

	var bindings [][]*v1.Binding
	for _, client := range o.pool {
		resp, err := client.ListBindings(context.Background(), request)
		if err != nil {
			bindings = append(bindings, make([]*v1.Binding, 0))
			continue
		}

		bindings = append(bindings, resp.Bindings)
	}

	return bindings, nil
}

func (o *Orchestrator) Count() int {
	return len(o.pool)
}
