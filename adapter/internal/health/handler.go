package health

import (
	"fmt"
	"net/http"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
)

// BindingStore returns a list of bindings.
type BindingStore interface {
	List() (bindings []*v1.Binding)
}

// Health handler will report the number of bindings in the BindingsStore.
type Health struct {
	store BindingStore
}

// NewHealth returns a new Health handler.
func NewHealth(s BindingStore) *Health {
	return &Health{
		store: s,
	}
}

// ServeHTTP implements the http.Handler interface.
func (h *Health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	count := len(h.store.List())
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(fmt.Sprintf(`{"drainCount": %d}` + "\n", count)))
}
