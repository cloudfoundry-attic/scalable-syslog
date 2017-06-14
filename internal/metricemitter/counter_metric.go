package metricemitter

import (
	"fmt"
	"sync/atomic"

	loggregator "code.cloudfoundry.org/go-loggregator"
)

type CounterMetric struct {
	name  string
	delta uint64
	tags  map[string]string
}

type MetricOption func(map[string]string)

func WithVersion(major, minor uint) MetricOption {
	return WithTags(map[string]string{
		"metric_version": fmt.Sprintf("%d.%d", major, minor),
	})
}

func WithTags(tags map[string]string) MetricOption {
	return func(c map[string]string) {
		for k, v := range tags {
			c[k] = v
		}
	}
}

func NewCounterMetric(name string, opts ...MetricOption) *CounterMetric {
	m := &CounterMetric{
		name: name,
		tags: make(map[string]string),
	}

	for _, opt := range opts {
		opt(m.tags)
	}

	return m
}

func (m *CounterMetric) Increment(c uint64) {
	atomic.AddUint64(&m.delta, c)
}

func (m *CounterMetric) GetDelta() uint64 {
	return atomic.LoadUint64(&m.delta)
}

func (m *CounterMetric) Emit(c LoggClient) {
	d := atomic.SwapUint64(&m.delta, 0)
	options := []loggregator.EmitCounterOption{loggregator.WithDelta(d)}
	for k, v := range m.tags {
		options = append(options, loggregator.WithEnvelopeStringTag(k, v))
	}

	c.EmitCounter(m.name, options...)
}
