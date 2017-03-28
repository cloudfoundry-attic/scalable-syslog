package binding

import (
	"golang.org/x/net/context"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
)

// BindingStore manages the bindings and respective subscriptions
type BindingStore interface {
	Add(binding *v1.Binding)
	Delete(binding *v1.Binding)
	List() (bindings []*v1.Binding)
}

// AdapterServer implements the v1.AdapterServer interface.
type AdapterServer struct {
	store BindingStore
}

// New returns a new AdapterServer.
func NewAdapterServer(store BindingStore) *AdapterServer {
	return &AdapterServer{
		store: store,
	}
}

// ListBindings returns a list of bindings from the binding manager
func (c *AdapterServer) ListBindings(ctx context.Context, req *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	return &v1.ListBindingsResponse{Bindings: c.store.List()}, nil
}

// CreateBinding adds a new binding to the binding manager.
func (c *AdapterServer) CreateBinding(ctx context.Context, req *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	c.store.Add(req.Binding)

	return &v1.CreateBindingResponse{}, nil
}

// DeleteBinding removes a binding from the binding manager.
func (c *AdapterServer) DeleteBinding(ctx context.Context, req *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	c.store.Delete(req.Binding)

	return &v1.DeleteBindingResponse{}, nil
}
