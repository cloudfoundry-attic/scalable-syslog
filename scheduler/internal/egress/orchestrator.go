// Package orchestrator orchestrates CUPS bindings to adapters.
package egress

import (
	"context"
	"log"
	"sync"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
)

type BindingReader interface {
	FetchBindings() (appBindings ingress.AppBindings, err error)
}

const maxWriteCount = 2

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	reader         BindingReader
	pool           AdapterPool
	once           sync.Once
	done           chan bool
	currentPoolPos int
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
			o.Create(actual, expected)
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

func (o *Orchestrator) Create(actual [][]*v1.Binding, expected ingress.AppBindings) {
	for appID, drainBinding := range expected {
		for _, drain := range drainBinding.Drains {
			b := &v1.Binding{
				Hostname: drainBinding.Hostname,
				AppId:    appID,
				Drain:    drain,
			}
			request := &v1.CreateBindingRequest{Binding: b}

			alreadyExist := 0
			for _, adapterBindings := range actual {
				for _, ab := range adapterBindings {
					if exists(expected, ab) {
						alreadyExist++
					}
				}
			}

			pool := o.pool.Sub(o.currentPoolPos, maxWriteCount-alreadyExist)
			for _, client := range pool {
				client.CreateBinding(context.Background(), request)
			}

			o.currentPoolPos += 2
			if o.currentPoolPos > len(o.pool) {
				o.currentPoolPos = 0
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
			_, err := client.DeleteBinding(context.Background(), request)
			if err != nil {
				log.Printf("delete binding failed: %v", err)
			}
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
