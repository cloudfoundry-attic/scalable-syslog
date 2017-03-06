// Package orchestrator orchestrates CUPS bindings to adapters.
package egress

import (
	"context"
	"errors"
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
			expectedBindings, err := o.reader.FetchBindings()
			if err != nil {
				continue
			}

			o.removeStaleBindings(expectedBindings)
			o.createBindings(expectedBindings)
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

func (o *Orchestrator) createBindings(expectedBindings ingress.AppBindings) {
	// TODO: this needs to diff against o.pool.List()
	for appID, cupsBinding := range expectedBindings {
		for _, drain := range cupsBinding.Drains {
			err := o.Create(&v1.Binding{
				Hostname: cupsBinding.Hostname,
				AppId:    appID,
				Drain:    drain,
			})

			if err != nil {
				log.Printf("orchestrator failed to write: %s", err)
			}
		}
	}
}

func (o *Orchestrator) removeStaleBindings(expectedBindings ingress.AppBindings) {
	actualBindings, err := o.List()
	if err != nil {
		log.Printf("Failed to get actual bindings: %s", err)
		return
	}

	var toDelete []*v1.Binding
	for _, adapterBindings := range actualBindings {
		for _, ab := range adapterBindings {
			if !exists(expectedBindings, ab) {
				toDelete = append(toDelete, ab)
			}
		}
	}

	for _, ab := range toDelete {
		o.DeleteAll(ab)
	}
}

func exists(actualBindings ingress.AppBindings, ab *v1.Binding) bool {
	b, ok := actualBindings[ab.AppId]
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

func (o *Orchestrator) Create(b *v1.Binding) error {
	request := &v1.CreateBindingRequest{
		Binding: b,
	}

	clientLen := len(o.pool)
	switch clientLen {
	case 0:
		return errors.New("No clients to create a binding against")
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

	return nil
}

func (o *Orchestrator) DeleteAll(b *v1.Binding) error {
	request := &v1.DeleteBindingRequest{
		Binding: b,
	}

	for _, client := range o.pool {
		client.DeleteBinding(context.Background(), request)
	}
	return nil
}

func (o *Orchestrator) Count() int {
	return len(o.pool)
}
