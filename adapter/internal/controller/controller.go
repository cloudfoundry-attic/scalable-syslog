package controller

import (
	"golang.org/x/net/context"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

// BindingsStore stores and lists bindings
type BindingStore interface {
	Add(binding *v1.Binding)
	Delete(binding *v1.Binding)
	List() (bindings []*v1.Binding)
}

// Controller implements the v1.AdapterServer interface.
type Controller struct {
	store BindingStore
}

// New returns a new Controller.
func New(s BindingStore) *Controller {
	return &Controller{
		store: s,
	}
}

// ListBindings returns a list of bindings from the bindings store.
func (c *Controller) ListBindings(ctx context.Context, req *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	return &v1.ListBindingsResponse{Bindings: c.store.List()}, nil
}

// CreateBinding adds a new binding to the bindings store.
func (c *Controller) CreateBinding(ctx context.Context, req *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	c.store.Add(req.Binding)

	return new(v1.CreateBindingResponse), nil
}

// DeleteBinding removes a binding from the bindings store.
func (c *Controller) DeleteBinding(ctx context.Context, req *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	c.store.Delete(req.Binding)

	return new(v1.DeleteBindingResponse), nil
}
