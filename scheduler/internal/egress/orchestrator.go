// Package orchestrator orchestrates CUPS bindings to adapters.
package egress

import (
	"log"
	"sync"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
)

type BindingReader interface {
	FetchBindings() (appBindings ingress.AppBindings, err error)
}

type BindingWriter interface {
	List() (bindings [][]*v1.Binding, err error)
	Create(binding *v1.Binding) (err error)
	DeleteAll(binding *v1.Binding) (err error)
}

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	reader     BindingReader
	repository BindingWriter
	once       sync.Once
	done       chan bool
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(r BindingReader, w BindingWriter) *Orchestrator {
	return &Orchestrator{
		reader:     r,
		repository: w,
		done:       make(chan bool),
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
			err := o.repository.Create(&v1.Binding{
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
	actualBindings, err := o.repository.List()
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
		o.repository.DeleteAll(ab)
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
