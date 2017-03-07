package egress

import (
	"context"
	"log"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
)

type DefaultAdapterService struct {
	pool           AdapterPool
	currentPoolPos int
}

func NewAdapterService(p AdapterPool) *DefaultAdapterService {
	return &DefaultAdapterService{
		pool: p,
	}
}

func (d *DefaultAdapterService) Create(actual BindingList, expected ingress.AppBindings) {
	for appID, drainBinding := range expected {
		for _, drain := range drainBinding.Drains {
			b := &v1.Binding{
				Hostname: drainBinding.Hostname,
				AppId:    appID,
				Drain:    drain,
			}
			request := &v1.CreateBindingRequest{Binding: b}

			alreadyExist := 0
			for _, adapterBindings := range actual {
				for _, ab := range adapterBindings {
					if exists(expected, ab) {
						alreadyExist++
					}
				}
			}

			pool := d.pool.Sub(d.currentPoolPos, maxWriteCount-alreadyExist)
			for _, client := range pool {
				client.CreateBinding(context.Background(), request)
			}

			d.currentPoolPos += 2
			if d.currentPoolPos > len(d.pool) {
				d.currentPoolPos = 0
			}
		}
	}
}

func (d *DefaultAdapterService) DeleteAll(actual BindingList, expected ingress.AppBindings) {
	var toDelete []*v1.Binding
	for _, adapterBindings := range actual {
		for _, ab := range adapterBindings {
			if !exists(expected, ab) {
				toDelete = append(toDelete, ab)
			}
		}
	}

	for _, ab := range toDelete {
		request := &v1.DeleteBindingRequest{
			Binding: ab,
		}

		for _, client := range d.pool {
			_, err := client.DeleteBinding(context.Background(), request)
			if err != nil {
				log.Printf("delete binding failed: %v", err)
			}
		}
	}
}

func exists(actual ingress.AppBindings, ab *v1.Binding) bool {
	b, ok := actual[ab.AppId]
	if !ok {
		return false
	}

	for _, d := range b.Drains {
		if d == ab.Drain {
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
