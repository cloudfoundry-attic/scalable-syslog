package binding

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

// BindingStore manages the bindings and respective subscriptions
type BindingStore interface {
	Add(binding *v1.Binding) error
	Delete(binding *v1.Binding)
	List() (bindings []*v1.Binding)
}

type HealthEmitter interface {
	SetCounter(map[string]int)
}

// AdapterServer implements the v1.AdapterServer interface.
type AdapterServer struct {
	store  BindingStore
	health HealthEmitter
}

// New returns a new AdapterServer.
func NewAdapterServer(store BindingStore, health HealthEmitter) *AdapterServer {
	return &AdapterServer{
		store:  store,
		health: health,
	}
}

// ListBindings returns a list of bindings from the binding manager
func (c *AdapterServer) ListBindings(ctx context.Context, req *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	return &v1.ListBindingsResponse{Bindings: c.store.List()}, nil
}

// CreateBinding adds a new binding to the binding manager.
func (c *AdapterServer) CreateBinding(ctx context.Context, req *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	err := c.store.Add(req.Binding)
	if err != nil {
		if err == ErrMaxBindingsExceeded {
			return nil, grpc.Errorf(codes.ResourceExhausted, "%s", err)
		}

		return nil, err
	}
	c.health.SetCounter(map[string]int{"drainCount": len(c.store.List())})

	return &v1.CreateBindingResponse{}, nil
}

// DeleteBinding removes a binding from the binding manager.
func (c *AdapterServer) DeleteBinding(ctx context.Context, req *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	c.store.Delete(req.Binding)
	c.health.SetCounter(map[string]int{"drainCount": len(c.store.List())})

	return &v1.DeleteBindingResponse{}, nil
}
