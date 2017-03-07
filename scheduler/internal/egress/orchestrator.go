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

type BindingList [][]*v1.Binding

type AdapterService interface {
	Create(actual BindingList, expected ingress.AppBindings)
	DeleteAll(actual BindingList, expected ingress.AppBindings)
	List() (BindingList, error)
}

const maxWriteCount = 2

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	reader  BindingReader
	service AdapterService
	once    sync.Once
	done    chan bool
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(r BindingReader, s AdapterService) *Orchestrator {
	return &Orchestrator{
		reader:  r,
		service: s,
		done:    make(chan bool),
	}
}

// Run starts the orchestrator.
func (o *Orchestrator) Run(interval time.Duration) {
	for {
		select {
		case <-time.Tick(interval):
			expected, err := o.reader.FetchBindings()
			if err != nil {
				log.Printf("fetch bindings failed with error: %s", err)
				continue
			}

			actual, err := o.service.List()
			if err != nil {
				log.Printf("failed to get actual bindings: %s", err)
				continue
			}

			o.service.DeleteAll(actual, expected)
			o.service.Create(actual, expected)
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
