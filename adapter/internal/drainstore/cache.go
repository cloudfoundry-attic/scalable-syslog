package drainstore

import (
	"sync"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

// Cache stores bindings.
type Cache struct {
	mu       sync.RWMutex
	bindings map[string]*v1.Binding
}

// NewCache returns a new Cache.
func NewCache() *Cache {
	return &Cache{
		bindings: make(map[string]*v1.Binding),
	}
}

// Add stores a new binding to the Cache.
func (c *Cache) Add(binding *v1.Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := buildKey(binding)
	c.bindings[key] = binding
}

// Delete removes a binding from the Cache. If the binding does not exist it
// is a nop
func (c *Cache) Delete(binding *v1.Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := buildKey(binding)
	delete(c.bindings, key)
}

// List returns a list of all the bindings in the Cache.
func (c *Cache) List() (bindings []*v1.Binding) {
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
