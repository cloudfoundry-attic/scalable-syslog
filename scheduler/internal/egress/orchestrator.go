// Package orchestrator writes syslog drain bindings to adapters.
package egress

import (
	"errors"
	"log"
	"math/rand"
	"time"

	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

const maxAdapters = 2

type BindingReader interface {
	FetchBindings() (appBindings []v1.Binding, invalid int, err error)
}

type HealthEmitter interface {
	SetCounter(c map[string]int)
}

type AdapterServicer interface {
	Transition(actual, desired State)
	List() State
}

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	addrs      []string
	reader     BindingReader
	service    AdapterServicer
	health     HealthEmitter
	drainGauge *metricemitter.GaugeMetric
}

type MetricEmitter interface {
	NewGaugeMetric(name, unit string, opts ...metricemitter.MetricOption) *metricemitter.GaugeMetric
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(
	addrs []string,
	r BindingReader,
	s AdapterServicer,
	h HealthEmitter,
	m MetricEmitter,
) *Orchestrator {
	// metric-documentation-v2: (scheduler.drains) Number of drains being
	// serviced by scalable syslog.
	drainGauge := m.NewGaugeMetric("drains", "count",
		metricemitter.WithVersion(2, 0),
	)

	return &Orchestrator{
		addrs:      addrs,
		reader:     r,
		service:    s,
		health:     h,
		drainGauge: drainGauge,
	}
}

// Run starts the orchestrator.
func (o *Orchestrator) Run(interval time.Duration) {
	for range time.Tick(interval) {
		freshBindings, blacklisted, err := o.reader.FetchBindings()
		if err != nil {
			log.Printf("fetch bindings failed with error: %s", err)
			continue
		}

		desired := desiredState(freshBindings, o.addrs)

		o.health.SetCounter(map[string]int{
			"drainCount":                   len(freshBindings),
			"blacklistedOrInvalidUrlCount": blacklisted,
		})
		o.drainGauge.Set(int64(len(freshBindings)))

		actual := o.service.List()
		o.service.Transition(actual, desired)
	}
}

// desiredState maps the current bindings onto adapters. Each binding gets
// mapped onto at most two adapters.
func desiredState(bindings []v1.Binding, addrs []string) State {
	r := rand.New(rand.NewSource(0))

	desired := State{}

	for _, b := range bindings {
		addr, remaining, err := sample(r, desired, addrs)
		if err != nil {
			continue
		}
		desired[addr] = append(desired[addr], b)

		addr, _, err = sample(r, desired, remaining)
		if err != nil {
			continue
		}
		desired[addr] = append(desired[addr], b)
	}

	return desired
}

func sample(r *rand.Rand, state State, addrs []string) (string, []string, error) {
	if len(addrs) == 0 {
		return "", nil, errors.New("empty addrs")
	}

	minAddrs := minKeys(state, addrs)
	sampled := minAddrs[r.Intn(len(minAddrs))]
	var remaining []string
	for _, addr := range addrs {
		if addr != sampled {
			remaining = append(remaining, addr)
		}
	}

	return sampled, remaining, nil
}

func minKeys(state State, addrs []string) []string {
	var minAddrs []string
	minLen := -1
	for _, addr := range addrs {
		l := len(state[addr])
		if minLen == -1 || l < minLen {
			minLen = l
			minAddrs = []string{addr}
		}
		if minLen == l {
			minAddrs = append(minAddrs, addr)
		}
	}
	return minAddrs
}
