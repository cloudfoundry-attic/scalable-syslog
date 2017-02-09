// Package drainstore stores the bindings from the CUPS provider.
package drainstore

import (
	"sync"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/cups"
)

// Cache stores the bindings.
type Cache struct {
	mu    sync.RWMutex
	count int
}

// NewCache returns a new Cache
func NewCache() *Cache {
	return &Cache{}
}

// Count returns the current number of drains.
func (c *Cache) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.count
}

// StoreBindings caches the current bindings from the CUPS provider.
func (c *Cache) StoreBindings(bindings map[string]cups.Binding) {
	var count int
	for _, v := range bindings {
		count += len(v.Drains)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.count = count
}
