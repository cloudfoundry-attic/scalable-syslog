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

func (d *DefaultAdapterService) CreateDelta(actual BindingList, expected ingress.AppBindings) {
	for appID, drainBinding := range expected {
		for _, drainURL := range drainBinding.Drains {
			b := &v1.Binding{
				Hostname: drainBinding.Hostname,
				AppId:    appID,
				Drain:    drainURL,
			}
			request := &v1.CreateBindingRequest{Binding: b}

			alreadyExist := actual.DrainCount(b)

			log.Printf(
				"creating new binding on adapter index=%d, number of writes=%d",
				d.currentPoolIdx,
				maxWriteCount-alreadyExist,
			)

			pool := d.pool.Subset(d.currentPoolIdx, maxWriteCount-alreadyExist)
			for _, client := range pool {
				client.CreateBinding(context.Background(), request)
			}

			d.currentPoolIdx += 1
			if d.currentPoolIdx >= len(d.pool) {
				d.currentPoolIdx = 0
			}
		}
	}
}

func (d *DefaultAdapterService) DeleteDelta(actual BindingList, expected ingress.AppBindings) {
	var toDelete []*v1.Binding
	for _, adapterBindings := range actual {
		for _, ab := range adapterBindings {
			if !exists(expected, ab) {
				toDelete = append(toDelete, ab)
			}
		}
	}
	log.Printf("deleting bindings count=%d", len(toDelete))

	for _, ab := range toDelete {
		request := &v1.DeleteBindingRequest{
			Binding: ab,
		}

		for _, client := range d.pool {
			_, err := client.DeleteBinding(context.Background(), request)
			if err != nil {
				log.Printf("delete binding failed: %s", err)
			}
		}
	}
}

func exists(expected ingress.AppBindings, ab *v1.Binding) bool {
	b, ok := expected[ab.AppId]
	if !ok {
		return false
	}

	for _, d := range b.Drains {
		if d == ab.Drain && b.Hostname == ab.Hostname {
			return true
		}
	}

	return false
}

func (d *DefaultAdapterService) List() (BindingList, error) {
	request := new(v1.ListBindingsRequest)

	var bindings BindingList
	for _, client := range d.pool {
		resp, err := client.ListBindings(context.Background(), request)
		if err != nil {
			bindings = append(bindings, make([]*v1.Binding, 0))
			continue
		}

		bindings = append(bindings, resp.Bindings)
	}

	return bindings, nil
}

func (d *DefaultAdapterService) Count() int {
	return len(d.pool)
}
