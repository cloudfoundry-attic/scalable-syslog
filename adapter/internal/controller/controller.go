package controller

import (
	"sync"

	"golang.org/x/net/context"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

type Controller struct {
	mu       sync.Mutex
	bindings []*v1.Binding
}

func New() *Controller {
	return new(Controller)
}

func (s *Controller) ListBindings(ctx context.Context, req *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &v1.ListBindingsResponse{Bindings: s.bindings}, nil
}

func (s *Controller) CreateBinding(ctx context.Context, req *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings = append(s.bindings, req.Binding)

	return new(v1.CreateBindingResponse), nil
}
