package egress

import (
	"context"
	"log"
	"time"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

// AdapterService is responsible for listing and transitioning the state of
// the adapters in the adapter pool.
type AdapterService struct {
	pool           AdapterPool
	currentPoolIdx int
}

// NewAdapterService returns a new AdapterService initialized with the Adapter
// pool.
func NewAdapterService(p AdapterPool) *AdapterService {
	return &AdapterService{
		pool: p,
	}
}

// List returns the state of all adapters and what drains they contain. If
// there are duplicate drains in any individual adapter the duplicates will be
// ignored.
func (d *AdapterService) List() State {
	state := State{}
	request := &v1.ListBindingsRequest{}

	for adapterAddr, client := range d.pool {
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := client.ListBindings(ctx, request)
		if err != nil {
			log.Printf("unable to retrieve bindings: %s", err)
			continue
		}
		bindingSet := make(map[v1.Binding]struct{})
		for _, b := range resp.Bindings {
			_, ok := bindingSet[*b]
			if ok {
				continue
			}
			bindingSet[*b] = struct{}{}
		}

		var bindings []v1.Binding
		for b := range bindingSet {
			bindings = append(bindings, b)
		}
		state[adapterAddr] = bindings
	}

	return state
}

// Transition issues commands to adapters to migrate them from one state to
// another.
func (d *AdapterService) Transition(actual, desired State) {
	createOps, deleteOps := delta(actual, desired)
	createOps.Create(d.pool)
	deleteOps.Delete(d.pool)
}

// State represents either the desired or actual state of all the adapters and
// what drains they currently maintain.
type State map[string][]v1.Binding

// Operations represent bindings that should be either deleted or created for
// each adapter
type Operations map[string][]v1.Binding

func (o Operations) Create(p AdapterPool) {
	for adapterAddr, ops := range o {
		client := p[adapterAddr]
		for _, op := range ops {
			request := &v1.CreateBindingRequest{
				Binding: &op,
			}
			ctx, _ := context.WithTimeout(context.Background(), time.Second)
			client.CreateBinding(ctx, request)
		}
	}
}

func (o Operations) Delete(p AdapterPool) {
	for adapterAddr, ops := range o {
		client := p[adapterAddr]
		for _, op := range ops {
			request := &v1.DeleteBindingRequest{
				Binding: &op,
			}
			ctx, _ := context.WithTimeout(context.Background(), time.Second)
			client.DeleteBinding(ctx, request)
		}
	}
}

// delta computes creation and deletion operations required to transtition
// from one state to another.
func delta(from, to State) (createOps, deleteOps Operations) {
	createOps = Operations{}
	deleteOps = Operations{}

	for addr, desiredBindings := range to {
		actualBindings, ok := from[addr]
		if !ok {
			createOps[addr] = desiredBindings
			continue
		}
		for _, desiredBinding := range desiredBindings {
			var present bool
			for _, actualBinding := range actualBindings {
				if desiredBinding == actualBinding {
					present = true
					break
				}
			}
			if !present {
				createOps[addr] = append(createOps[addr], desiredBinding)
			}
		}
	}

	for addr, actualBindings := range from {
		desiredBindings, ok := to[addr]
		if !ok {
			deleteOps[addr] = actualBindings
			continue
		}
		for _, actualBinding := range actualBindings {
			var present bool
			for _, desiredBinding := range desiredBindings {
				if desiredBinding == actualBinding {
					present = true
					break
				}
			}
			if !present {
				deleteOps[addr] = append(deleteOps[addr], actualBinding)
			}
		}
	}

	return createOps, deleteOps
}
