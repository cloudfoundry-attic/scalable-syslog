package controller

import (
	"golang.org/x/net/context"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

// BindingManager manages the bindings and respective subscriptions
type BindingManager interface {
	Add(binding *v1.Binding)
	Delete(binding *v1.Binding)
	List() (bindings []*v1.Binding)
}

// Controller implements the v1.AdapterServer interface.
type Controller struct {
	manager BindingManager
}

// New returns a new Controller.
func New(m BindingManager) *Controller {
	return &Controller{
		manager: m,
	}
}

// ListBindings returns a list of bindings from the binding manager
func (c *Controller) ListBindings(ctx context.Context, req *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	return &v1.ListBindingsResponse{Bindings: c.manager.List()}, nil
}

// CreateBinding adds a new binding to the binding manager.
func (c *Controller) CreateBinding(ctx context.Context, req *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	c.manager.Add(req.Binding)

	return new(v1.CreateBindingResponse), nil
}

// DeleteBinding removes a binding from the binding manager.
func (c *Controller) DeleteBinding(ctx context.Context, req *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	c.manager.Delete(req.Binding)

	return new(v1.DeleteBindingResponse), nil
}
