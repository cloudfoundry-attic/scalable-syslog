package egress

import (
	"context"
	"log"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
)

type DefaultAdapterService struct {
	pool           AdapterPool
	currentPoolIdx int
}

// maxWriteCount defines the number of adapters to which to write
// syslog drain bindings
const maxWriteCount = 2

func NewAdapterService(p AdapterPool) *DefaultAdapterService {
	return &DefaultAdapterService{
		pool: p,
	}
}

func (d *DefaultAdapterService) CreateDelta(actual ingress.Bindings, expected ingress.Bindings) {
	for _, expectedBinding := range expected {
		b := &v1.Binding{
			Hostname: expectedBinding.Hostname,
			AppId:    expectedBinding.AppId,
			Drain:    expectedBinding.Drain,
		}
		request := &v1.CreateBindingRequest{Binding: b}

		targetWriteCount := min(maxWriteCount, len(d.pool))
		drainCount := actual.DrainCount(expectedBinding)
		actualCreateCount := targetWriteCount - drainCount

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

func (d *DefaultAdapterService) DeleteDelta(actual ingress.Bindings, expected ingress.Bindings) {
	var toDelete ingress.Bindings
	for _, binding := range actual {
		if !exists(expected, binding) {
			toDelete = append(toDelete, binding)
		}
	}
	log.Printf("deleting bindings count=%d", len(toDelete))

	for _, binding := range toDelete {
		request := &v1.DeleteBindingRequest{
			Binding: &v1.Binding{
				Hostname: binding.Hostname,
				AppId:    binding.AppId,
				Drain:    binding.Drain,
			},
		}

		for _, client := range d.pool {
			_, err := client.DeleteBinding(context.Background(), request)
			if err != nil {
				log.Printf("delete binding failed: %s", err)
			}
		}
	}
}

func exists(expected ingress.Bindings, ab v1.Binding) bool {
	for _, b := range expected {
		if b.Drain == ab.Drain && b.Hostname == ab.Hostname {
			return true
		}
	}

	return false
}

func (d *DefaultAdapterService) List() (ingress.Bindings, error) {
	request := &v1.ListBindingsRequest{}

	var bindings ingress.Bindings

	for _, client := range d.pool {
		resp, err := client.ListBindings(context.Background(), request)
		if err != nil {
			continue
		}
		// TODO remove conversion by switching this to v1.Binding
		for _, b := range resp.Bindings {
			bindings = append(bindings, v1.Binding{
				AppId:    b.AppId,
				Hostname: b.Hostname,
				Drain:    b.Drain,
			})
		}
	}

	return bindings, nil
}

func (d *DefaultAdapterService) Count() int {
	return len(d.pool)
}
