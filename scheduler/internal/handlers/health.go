// Package handlers contains the HTTP handlers that are used
// for easy debugging and health checks for operators.
package handlers

import (
	"fmt"
	"net/http"
)

// DrainCounter returns the current number of drains.
type DrainCounter interface {
	Count() (drains int)
}

// The Health handler will report the number of drains back to user.
type Health struct {
	counter DrainCounter
}

// NewHealth returns a new Health handler.
func NewHealth(c DrainCounter) *Health {
	return &Health{
		counter: c,
	}
}

// Handle implements the http.Handler interface.
func (h *Health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	output := fmt.Sprintf(`{"drainCount": %d}`, h.counter.Count())
	w.Write([]byte(output))
}
