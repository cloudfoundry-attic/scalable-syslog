package bindingmanager

import (
	"sync"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

// BindingManager stores bindings.
type BindingManager struct {
	mu       sync.RWMutex
	bindings map[string]*v1.Binding
}

// New returns a new Binding Manager.
func New() *BindingManager {
	return &BindingManager{
		bindings: make(map[string]*v1.Binding),
	}
}

// Add stores a new binding to the Binding Manager.
func (c *BindingManager) Add(binding *v1.Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := buildKey(binding)
	c.bindings[key] = binding
}

// Delete removes a binding from the Binding Manager. If the binding does not exist it
// is a nop
func (c *BindingManager) Delete(binding *v1.Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := buildKey(binding)
	delete(c.bindings, key)
}

// List returns a list of all the bindings in the Binding Manager.
func (c *BindingManager) List() (bindings []*v1.Binding) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, b := range c.bindings {
		bindings = append(bindings, b)
	}

	return bindings
}

func buildKey(binding *v1.Binding) (key string) {
	return binding.AppId + binding.Hostname + binding.Drain
}
