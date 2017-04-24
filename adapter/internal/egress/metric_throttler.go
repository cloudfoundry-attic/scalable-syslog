package egress

import "time"

type metricThrottler struct {
	emitCount int
	emitTime  time.Time
	duration  time.Duration
}

func NewMetricThrottler() *metricThrottler {
	return &metricThrottler{
		emitTime: time.Now(),
		duration: 5*time.Second,
	}
}

func (m *metricThrottler) Emit(fn func(int)) {
	m.emitCount++
	if m.emitCount >= 1000 || time.Since(m.emitTime) >= m.duration {
		fn(m.emitCount)
		m.emitTime = time.Now()
		m.emitCount = 0
	}
}
