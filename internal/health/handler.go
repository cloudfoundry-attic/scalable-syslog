// Package handlers contains the HTTP handlers that are used
// for easy debugging and health checks for operators.
package health

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

// The Health handler will report the number of drains back to user.
type Health struct {
	counts map[string]int
	mu     sync.RWMutex
}

// NewHealth returns a new Health handler.
func NewHealth() *Health {
	return &Health{
		counts: make(map[string]int),
	}
}

// Handle implements the http.Handler interface.
func (h *Health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	h.mu.RLock()
	jsonCounts, err := json.Marshal(h.counts)
	h.mu.RUnlock()

	if err != nil {
		log.Printf("unable to marshal counts: %s", err)
	}
	w.Write(jsonCounts)
}

func (h *Health) SetCounter(c map[string]int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for k, v := range c {
		h.counts[k] = v
	}
}
