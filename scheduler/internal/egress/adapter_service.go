package egress

import (
	"context"
	"log"
	"time"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
)

// DefaultAdapterService is responsible for maintaining the state of the
// syslog drain bindings for the adapters in the adapter pool. Each syslog
// drain binding is delegated to two adapters for load balancing and
// availability purposes.
type DefaultAdapterService struct {
	pool           AdapterPool
	currentPoolIdx int
}

// maxWriteCount defines the number of adapters to which to write
// syslog drain bindings
const maxWriteCount = 2

// NewAdapterService returns a new DefaultAdapterService initialized with the
// Adapter pool.
func NewAdapterService(p AdapterPool) *DefaultAdapterService {
	return &DefaultAdapterService{
		pool: p,
	}
}

// CreateDelta sends a request to at most two (maxWriteCount) adapters to
// create new bindings that are expected.
func (d *DefaultAdapterService) CreateDelta(actual ingress.Bindings, expected ingress.Bindings) {
	for _, expectedBinding := range expected {
		request := &v1.CreateBindingRequest{
			Binding: &expectedBinding,
		}

		targetWriteCount := min(maxWriteCount, len(d.pool))
		drainCount := actual.DrainCount(expectedBinding)
		actualCreateCount := targetWriteCount - drainCount
		if actualCreateCount < 1 {
			continue
		}

		log.Printf(
			"creating new binding on adapter index=%d, number of writes=%d",
			d.currentPoolIdx,
			actualCreateCount,
		)

		pool := d.pool.Subset(d.currentPoolIdx, actualCreateCount)
		for _, client := range pool {
			client.CreateBinding(context.Background(), request)
		}

		d.currentPoolIdx += 1
		if d.currentPoolIdx >= len(d.pool) {
			d.currentPoolIdx = 0
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// DeleteDelta sends a request to delete the bindings that are no longer
// expected.
func (d *DefaultAdapterService) DeleteDelta(actual ingress.Bindings, expected ingress.Bindings) {
	var toDelete ingress.Bindings
	for _, binding := range actual {
		if expected.DrainCount(binding) == 0 {
			toDelete = append(toDelete, binding)
		}
	}
	log.Printf("deleting bindings count=%d", len(toDelete))

	for _, binding := range toDelete {
		request := &v1.DeleteBindingRequest{
			Binding: &binding,
		}

		for _, client := range d.pool {
			_, err := client.DeleteBinding(context.Background(), request)
			if err != nil {
				log.Printf("delete binding failed: %s", err)
			}
		}
	}
}

// List returns a list of unique bindings per adapter. Duplicate bindings may
// be returned because there may be multiple adapters with the same binding.
func (d *DefaultAdapterService) List() ingress.Bindings {
	var allBindings ingress.Bindings
	request := &v1.ListBindingsRequest{}

	for _, client := range d.pool {
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		resp, err := client.ListBindings(ctx, request)
		if err != nil {
			log.Printf("unable to retrieve bindings: %s", err)
			continue
		}
		bindings := make(map[v1.Binding]struct{})
		for _, b := range resp.Bindings {
			_, ok := bindings[*b]
			if ok {
				continue
			}
			bindings[*b] = struct{}{}
			allBindings = append(allBindings, *b)
		}
	}

	return allBindings
}
