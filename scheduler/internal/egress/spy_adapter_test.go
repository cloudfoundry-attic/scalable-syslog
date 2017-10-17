package egress_test

import (
	"sync"

	"golang.org/x/net/context"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

func newSpyAdapterServer() *spyAdapterServer {
	return &spyAdapterServer{
		ActualCreateBindingRequest: make(chan *v1.CreateBindingRequest, 10),
		ActualDeleteBindingRequest: make(chan *v1.DeleteBindingRequest, 10),
	}
}

type spyAdapterServer struct {
	ActualCreateBindingRequest chan *v1.CreateBindingRequest
	ActualDeleteBindingRequest chan *v1.DeleteBindingRequest
	mu                         sync.Mutex
	Bindings                   []*v1.Binding
}

func (t *spyAdapterServer) ListBindings(context.Context, *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return &v1.ListBindingsResponse{Bindings: t.Bindings}, nil
}

func (t *spyAdapterServer) CreateBinding(c context.Context, r *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	t.mu.Lock()
	t.Bindings = append(t.Bindings, r.Binding)
	t.mu.Unlock()

	t.ActualCreateBindingRequest <- r

	return new(v1.CreateBindingResponse), nil
}

func (t *spyAdapterServer) DeleteBinding(c context.Context, r *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	t.mu.Lock()
	for i, b := range t.Bindings {
		if b.AppId == r.Binding.AppId && b.Hostname == r.Binding.Hostname && b.Drain == r.Binding.Drain {
			continue
		}
		t.Bindings = append(t.Bindings[:i], t.Bindings[i+1:]...)
		break
	}
	t.mu.Unlock()

	t.ActualDeleteBindingRequest <- r

	return new(v1.DeleteBindingResponse), nil
}
