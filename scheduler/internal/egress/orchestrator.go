// Package orchestrator writes syslog drain bindings to adapters.
package egress

import (
	"log"
	"time"

	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
)

type BindingReader interface {
	FetchBindings() (appBindings ingress.Bindings, invalid int, err error)
}

type HealthEmitter interface {
	SetCounter(c map[string]int)
}

type AdapterService interface {
	CreateDelta(actual ingress.Bindings, expected ingress.Bindings)
	DeleteDelta(actual ingress.Bindings, expected ingress.Bindings)
	List() (ingress.Bindings, error)
}

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	reader       BindingReader
	service      AdapterService
	health       HealthEmitter
	egressClient *metricemitter.CounterMetric
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(
	r BindingReader,
	s AdapterService,
	h HealthEmitter,
	m metricemitter.MetricClient,
) *Orchestrator {
	return &Orchestrator{
		reader:  r,
		service: s,
		health:  h,
		egressClient: m.NewCounterMetric(
			"drains",
			metricemitter.WithVersion(2, 0),
		),
	}
}

// Run starts the orchestrator.
func (o *Orchestrator) Run(interval time.Duration) {
	for range time.Tick(interval) {
		expected, blacklisted, err := o.reader.FetchBindings()
		if err != nil {
			log.Printf("fetch bindings failed with error: %s", err)
			continue
		}

		o.health.SetCounter(map[string]int{
			"drainCount":                   len(expected),
			"blacklistedOrInvalidUrlCount": blacklisted,
		})

		o.egressClient.Increment(uint64(len(expected)))

		actual, err := o.service.List()
		if err != nil {
			log.Printf("failed to get actual bindings: %s", err)
			continue
		}

		o.service.DeleteDelta(actual, expected)
		o.service.CreateDelta(actual, expected)
	}
}
