// Package handlers contains the HTTP handlers that are used
// for easy debugging and health checks for operators.
package handlers

import (
	"fmt"
	"net/http"
)

// Counter provides numerical information about an object's health.
type Counter interface {
	Count() int
}

// The Health handler will report the number of drains back to user.
type Health struct {
	drainCounter   Counter
	adapterCounter Counter
}

// NewHealth returns a new Health handler.
func NewHealth(c Counter, a Counter) *Health {
	return &Health{
		drainCounter:   c,
		adapterCounter: a,
	}
}

// Handle implements the http.Handler interface.
func (h *Health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	output := fmt.Sprintf(
		`{"drainCount": %d,"adapterCount": %d}`,
		h.drainCounter.Count(),
		h.adapterCounter.Count(),
	)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(output))
}
