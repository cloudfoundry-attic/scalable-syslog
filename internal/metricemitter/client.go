package metricemitter

import (
	"time"

	loggregator "code.cloudfoundry.org/go-loggregator"
)

type MetricClient interface {
	NewCounterMetric(name string, opts ...MetricOption) *CounterMetric
}

type LoggClient interface {
	EmitCounter(name string, opts ...loggregator.EmitCounterOption)
	EmitGauge(opts ...loggregator.EmitGaugeOption)
}

type client struct {
	loggClient LoggClient

	pulseInterval time.Duration
}

type ClientOption func(*client)

func WithPulseInterval(d time.Duration) ClientOption {
	return func(c *client) {
		c.pulseInterval = d
	}
}

func NewClient(c LoggClient, opts ...ClientOption) *client {
	client := &client{
		pulseInterval: 5 * time.Second,
		loggClient:    c,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

func (c *client) NewCounterMetric(name string, opts ...MetricOption) *CounterMetric {
	m := NewCounterMetric(name, opts...)
	go c.pulse(m)

	return m
}

func (c *client) NewGaugeMetric(name, unit string, opts ...MetricOption) *GaugeMetric {
	g := NewGaugeMetric(name, unit, opts...)
	go c.pulse(g)

	return g
}

type emitter interface {
	Emit(c LoggClient)
}

func (c *client) pulse(e emitter) {
	for range time.Tick(c.pulseInterval) {
		e.Emit(c.loggClient)
	}
}
