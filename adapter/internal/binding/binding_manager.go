package binding

import (
	"errors"
	"sync"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

var (
	ErrMaxBindingsExceeded = errors.New("Max bindings for adapter exceeded")
)

// MetricClient is used to emit metrics.
type MetricClient interface {
	NewGaugeMetric(string, string, ...pulseemitter.MetricOption) pulseemitter.GaugeMetric
	NewCounterMetric(name string, opts ...pulseemitter.MetricOption) pulseemitter.CounterMetric
}

// BindingManager stores binding subscriptions.
type BindingManager struct {
	mu            sync.RWMutex
	subscriptions map[v1.Binding]subscription
	subscriber    Subscriber
	maxBindings   int

	drainBindingsMetric    pulseemitter.GaugeMetric
	rejectedBindingsMetric pulseemitter.CounterMetric

	logClient   LogClient
	sourceIndex string
}

// Subscriber reads and writes logs for a specific binding.
type Subscriber interface {
	Start(binding *v1.Binding) (stopFunc func())
}

// LogClient is used to emit logs.
type LogClient interface {
	EmitLog(message string, opts ...loggregator.EmitLogOption)
}

type subscription struct {
	binding     *v1.Binding
	unsubscribe func()
}

// New returns a new Binding Manager.
func NewBindingManager(
	s Subscriber,
	mc MetricClient,
	lc LogClient,
	sourceIndex string,
	opts ...BindingManagerOption,
) *BindingManager {
	dbm := mc.NewGaugeMetric("drain_bindings", "bindings")
	rbm := mc.NewCounterMetric("rejected_bindings")

	b := &BindingManager{
		subscriptions:          make(map[v1.Binding]subscription),
		subscriber:             s,
		maxBindings:            500,
		drainBindingsMetric:    dbm,
		rejectedBindingsMetric: rbm,
		logClient:              lc,
		sourceIndex:            sourceIndex,
	}

	for _, o := range opts {
		o(b)
	}

	return b
}

// Add stores a new binding subscription to the Binding Manager.
func (c *BindingManager) Add(binding *v1.Binding) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := *binding
	if _, ok := c.subscriptions[key]; !ok {
		if len(c.subscriptions) >= c.maxBindings {
			c.rejectedBindingsMetric.Increment(1)
			c.logClient.EmitLog(
				"Syslog adapter has failed to schedule your drain stream",
				loggregator.WithAppInfo(binding.AppId, "LGR", c.sourceIndex),
			)

			return ErrMaxBindingsExceeded
		}

		unsub := c.subscriber.Start(binding)
		c.subscriptions[key] = subscription{
			binding:     binding,
			unsubscribe: unsub,
		}
	}

	c.drainBindingsMetric.Set(float64(len(c.subscriptions)))

	return nil
}

// Delete removes a binding subscription from the Binding Manager.
// It also unsubscribes the binding subscription.
// If the binding does not exist it is a nop.
func (c *BindingManager) Delete(binding *v1.Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := *binding
	s, ok := c.subscriptions[key]
	if ok {
		s.unsubscribe()
	}

	delete(c.subscriptions, key)

	c.drainBindingsMetric.Set(float64(len(c.subscriptions)))
}

// List returns a list of all the bindings in the Binding Manager.
func (c *BindingManager) List() []*v1.Binding {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var bindings []*v1.Binding

	for _, b := range c.subscriptions {
		bindings = append(bindings, b.binding)
	}

	return bindings
}

// BindingManagerOption is a function that can be used to configure optional
// settings on a BindingManager.
type BindingManagerOption func(*BindingManager)

// WithMaxBindings sets the maximum number of allowed bindings.
func WithMaxBindings(max int) BindingManagerOption {
	return func(m *BindingManager) {
		m.maxBindings = max
	}
}
