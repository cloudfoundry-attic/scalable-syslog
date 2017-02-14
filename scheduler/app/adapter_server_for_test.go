package app_test

import (
	"golang.org/x/net/context"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

func NewTestAdapterServer() *testAdapterServer {
	return &testAdapterServer{
		ActualCreateBindingRequest: make(chan *v1.CreateBindingRequest, 10),
		ActualDeleteBindingRequest: make(chan *v1.DeleteBindingRequest, 10),
	}
}

type testAdapterServer struct {
	ActualCreateBindingRequest chan *v1.CreateBindingRequest
	ActualDeleteBindingRequest chan *v1.DeleteBindingRequest
	Bindings                   []*v1.Binding
}

func (t *testAdapterServer) ListBindings(context.Context, *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	return &v1.ListBindingsResponse{Bindings: t.Bindings}, nil
}

func (t *testAdapterServer) CreateBinding(c context.Context, r *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	t.Bindings = []*v1.Binding{r.Binding}

	t.ActualCreateBindingRequest <- r

	return new(v1.CreateBindingResponse), nil
}

func (t *testAdapterServer) DeleteBinding(c context.Context, r *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	t.ActualDeleteBindingRequest <- r

	return new(v1.DeleteBindingResponse), nil
}
