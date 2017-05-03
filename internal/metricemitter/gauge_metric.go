package metricemitter

import (
	"sync/atomic"
	"time"

	v2 "code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
)

type GaugeMetric struct {
	name     string
	unit     string
	sourceID string
	value    int64
	tags     map[string]*v2.Value
}

func NewGaugeMetric(name, unit, sourceID string, opts ...MetricOption) *GaugeMetric {
	g := &GaugeMetric{
		name:     name,
		unit:     unit,
		sourceID: sourceID,
		tags:     make(map[string]*v2.Value),
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func (g *GaugeMetric) Set(number int64) {
	atomic.SwapInt64(&g.value, number)
}

func (g *GaugeMetric) WithEnvelope(fn func(*v2.Envelope) error) error {
	return fn(g.toEnvelope())
}

func (g *GaugeMetric) toEnvelope() *v2.Envelope {
	metrics := make(map[string]*v2.GaugeValue)
	metrics[g.name] = &v2.GaugeValue{
		Unit:  g.unit,
		Value: float64(atomic.LoadInt64(&g.value)),
	}

	return &v2.Envelope{
		SourceId:  g.sourceID,
		Timestamp: time.Now().UnixNano(),
		Message: &v2.Envelope_Gauge{
			Gauge: &v2.Gauge{
				Metrics: metrics,
			},
		},
		Tags: g.tags,
	}
}

func (g *GaugeMetric) setTag(k, v string) {
	g.tags[k] = &v2.Value{
		Data: &v2.Value_Text{
			Text: v,
		},
	}
}
