// Package orchestrator writes syslog drain bindings to adapters.
package egress

import (
	"context"
	"log"
	"time"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	orchestrator "code.cloudfoundry.org/go-orchestrator"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

const maxAdapters = 2

type BindingReader interface {
	FetchBindings() (appBindings []v1.Binding, invalid int, err error)
}

type HealthEmitter interface {
	SetCounter(c map[string]int)
}

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	reader     BindingReader
	comm       Communicator
	orch       *orchestrator.Orchestrator
	health     HealthEmitter
	drainGauge pulseemitter.GaugeMetric
}

type Communicator interface {
	// List returns the workload from the given adapter.
	List(ctx context.Context, adapter interface{}) ([]interface{}, error)

	// Add adds the given task to the worker. The error only logged (for now).
	// It is assumed that if the worker returns an error trying to update, the
	// next term will fix the problem and move the task elsewhere.
	Add(ctx context.Context, adapter, binding interface{}) error

	// Removes the given task from the worker. The error is only logged (for
	// now). It is assumed that if the worker is returning an error, then it
	// is either not doing the task because the worker is down, or there is a
	// network partition and a future term will fix the problem.
	Remove(ctx context.Context, adapter, binding interface{}) error
}

type MetricEmitter interface {
	NewGaugeMetric(name, unit string, opts ...pulseemitter.MetricOption) pulseemitter.GaugeMetric
	NewCounterMetric(name string, opts ...pulseemitter.MetricOption) pulseemitter.CounterMetric
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(
	adapterPool AdapterPool,
	r BindingReader,
	c Communicator,
	h HealthEmitter,
	m MetricEmitter,
) *Orchestrator {
	// metric-documentation-v2: (scheduler.drains) Number of drains being
	// serviced by scalable syslog.
	drainGauge := m.NewGaugeMetric("drains", "count",
		pulseemitter.WithVersion(2, 0),
	)

	adapterGauge := m.NewGaugeMetric("adapters", "count",
		pulseemitter.WithVersion(2, 0),
	)

	orch := orchestrator.New(c,
		orchestrator.WithStats(func(s orchestrator.TermStats) {
			adapterGauge.Set(float64(s.WorkerCount))
		}),
	)
	for _, client := range adapterPool.Pool {
		orch.AddWorker(client)
	}

	return &Orchestrator{
		reader:     r,
		comm:       c,
		health:     h,
		drainGauge: drainGauge,
		orch:       orch,
	}
}

func (o *Orchestrator) NextTerm() {
	freshBindings, blacklisted, err := o.reader.FetchBindings()
	if err != nil {
		log.Printf("fetch bindings failed with error: %s", err)
		return
	}

	o.health.SetCounter(map[string]int{
		"drainCount":                   len(freshBindings),
		"blacklistedOrInvalidUrlCount": blacklisted,
	})
	o.drainGauge.Set(float64(len(freshBindings)))

	var tasks []orchestrator.Task
	for _, b := range freshBindings {
		tasks = append(tasks, orchestrator.Task{
			Name:      b,
			Instances: maxAdapters,
		})
	}

	o.orch.UpdateTasks(tasks)
	o.orch.NextTerm(context.Background())
}

// Run starts the orchestrator.
func (o *Orchestrator) Run(interval time.Duration) {
	for range time.Tick(interval) {
		o.NextTerm()
	}
}
