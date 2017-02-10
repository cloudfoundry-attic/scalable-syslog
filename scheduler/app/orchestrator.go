// Package orchestrator orchestrates CUPS bindings to adapters.
package app

import (
	"log"
	"sync"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

type BindingReader interface {
	FetchBindings() (AppBindings, error)
}

type BindingWriter interface {
	Write(*v1.Binding) error
}

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	reader BindingReader
	writer BindingWriter
	once   sync.Once
	done   chan bool
}

// New creates an orchestrator.
func NewOrchestrator(r BindingReader, w BindingWriter) *Orchestrator {
	return &Orchestrator{
		reader: r,
		writer: w,
		done:   make(chan bool),
	}
}

// Start the orchestrator
func (o *Orchestrator) Run(interval time.Duration) {
	for {
		select {
		case <-time.Tick(interval):
			bindings, err := o.reader.FetchBindings()
			if err != nil {
				continue
			}

			for appID, cupsBinding := range bindings {
				for _, drain := range cupsBinding.Drains {
					err := o.writer.Write(&v1.Binding{
						Hostname: cupsBinding.Hostname,
						AppId:    appID,
						Drain:    drain,
					})

					if err != nil {
						log.Printf("orchestrator failed to write: %s", err)
					}
				}
			}
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
