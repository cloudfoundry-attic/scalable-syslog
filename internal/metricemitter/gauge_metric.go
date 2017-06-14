package metricemitter

import (
	"sync/atomic"

	loggregator "code.cloudfoundry.org/go-loggregator"
)

type GaugeMetric struct {
	name  string
	unit  string
	value int64
	tags  map[string]string
}

func NewGaugeMetric(name, unit string, opts ...MetricOption) *GaugeMetric {
	g := &GaugeMetric{
		name: name,
		unit: unit,
		tags: make(map[string]string),
	}

	for _, opt := range opts {
		opt(g.tags)
	}

	return g
}

func (g *GaugeMetric) Set(number int64) {
	atomic.SwapInt64(&g.value, number)
}

func (g *GaugeMetric) Emit(c LoggClient) {
	options := []loggregator.EmitGaugeOption{
		loggregator.WithGaugeValue(
			g.name,
			float64(atomic.LoadInt64(&g.value)),
			g.unit,
		)}
	for k, v := range g.tags {
		options = append(options, loggregator.WithEnvelopeStringTag(k, v))
	}

	c.EmitGauge(options...)
}
