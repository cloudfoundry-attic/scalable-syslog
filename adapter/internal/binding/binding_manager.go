package binding

import (
	"sync"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
)

// BindingManager stores binding subscriptions.
type BindingManager struct {
	mu            sync.RWMutex
	subscriptions map[v1.Binding]subscription
	subscriber    Subscriber
}

// Subscriber reads and writes logs for a specific binding.
type Subscriber interface {
	Start(binding *v1.Binding) (stopFunc func())
}

type subscription struct {
	binding     *v1.Binding
	unsubscribe func()
}

// New returns a new Binding Manager.
func NewBindingManager(s Subscriber) *BindingManager {
	return &BindingManager{
		subscriptions: make(map[v1.Binding]subscription),
		subscriber:    s,
	}
}

// Add stores a new binding subscription to the Binding Manager.
func (c *BindingManager) Add(binding *v1.Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := *binding
	if _, ok := c.subscriptions[key]; !ok {
		c.subscriptions[key] = subscription{
			binding:     binding,
			unsubscribe: c.subscriber.Start(binding),
		}
	}
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
