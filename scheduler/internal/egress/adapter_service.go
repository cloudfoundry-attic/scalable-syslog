package egress

import (
	"log"
	"time"

	"golang.org/x/net/context"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

// AdapterService is responsible for listing and transitioning the state of
// the adapters in the adapter pool.
type AdapterService struct {
	pool           AdapterPool
	comm           Communicator
	currentPoolIdx int
}

// NewAdapterService returns a new AdapterService initialized with the Adapter
// pool.
func NewAdapterService(p AdapterPool, c Communicator) *AdapterService {
	return &AdapterService{
		pool: p,
		comm: c,
	}
}

type Communicator interface {
	// List returns the workload from the given adapter.
	List(ctx context.Context, adapter interface{}) ([]interface{}, error)

	// Add adds the given task to the worker. The error only logged (for now).
	// It is assumed that if the worker returns an error trying to update, the
	// next term will fix the problem and move the task elsewhere.
	Add(ctx context.Context, adapter, binding interface{}) error

	// Removes the given task from the worker. The error is only logged (for
	// now). It is assumed that if the worker is returning an error, then it
	// is either not doing the task because the worker is down, or there is a
	// network partition and a future term will fix the problem.
	Remove(ctx context.Context, adapter, binding interface{}) error
}

// List returns the state of all adapters and what drains they contain. If
// there are duplicate drains in any individual adapter the duplicates will be
// ignored.
func (d *AdapterService) List() State {
	state := State{}
	for adapterAddr, client := range d.pool {
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := d.comm.List(ctx, client)
		if err != nil {
			log.Printf("unable to retrieve bindings from %s: %s", adapterAddr, err)
			continue
		}

		bindingSet := make(map[v1.Binding]struct{})
		for _, b := range resp {
			bb := b.(v1.Binding)
			_, ok := bindingSet[bb]
			if ok {
				continue
			}
			bindingSet[bb] = struct{}{}
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
	createOps.Create(d.pool, d.comm)
	deleteOps.Delete(d.pool, d.comm)
}

// State represents either the desired or actual state of all the adapters and
// what drains they currently maintain.
type State map[string][]v1.Binding

// Operations represent bindings that should be either deleted or created for
// each adapter
type Operations map[string][]v1.Binding

func (o Operations) Create(p AdapterPool, c Communicator) {
	for adapterAddr, ops := range o {
		client := p[adapterAddr]
		for _, op := range ops {
			ctx, _ := context.WithTimeout(context.Background(), time.Second)
			err := c.Add(ctx, client, op)
			if err != nil {
				log.Printf("Failed to create binding on %s: %s", adapterAddr, err)
			}
		}
	}
}

func (o Operations) Delete(p AdapterPool, c Communicator) {
	for adapterAddr, ops := range o {
		client := p[adapterAddr]
		for _, op := range ops {
			ctx, _ := context.WithTimeout(context.Background(), time.Second)
			err := c.Remove(ctx, client, op)
			if err != nil {
				log.Printf("Failed to delete binding on %s: %s", adapterAddr, err)
			}
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
