package testhelper

import "code.cloudfoundry.org/scalable-syslog/internal/metricemitter"

type SpyMetricClient struct {
	counterMetrics map[string]*metricemitter.CounterMetric

	GaugeMetric *metricemitter.GaugeMetric
}

func NewMetricClient() *SpyMetricClient {
	return &SpyMetricClient{
		counterMetrics: make(map[string]*metricemitter.CounterMetric),
	}
}

func (s *SpyMetricClient) NewCounterMetric(name string, opts ...metricemitter.MetricOption) *metricemitter.CounterMetric {
	m := &metricemitter.CounterMetric{}
	s.counterMetrics[name] = m

	return m
}

func (s *SpyMetricClient) NewGaugeMetric(name, unit string, opts ...metricemitter.MetricOption) *metricemitter.GaugeMetric {
	s.GaugeMetric = metricemitter.NewGaugeMetric(name, unit, "spy-client", opts...)

	return s.GaugeMetric
}

func (s *SpyMetricClient) GetDelta(name string) uint64 {
	return s.counterMetrics[name].GetDelta()
}
