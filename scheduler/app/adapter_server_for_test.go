package app_test

import (
	"golang.org/x/net/context"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

func NewTestAdapterServer() *testAdapterServer {
	return &testAdapterServer{
		ActualCreateBindingRequest: make(chan *v1.CreateBindingRequest, 10),
	}
}

type testAdapterServer struct {
	ActualCreateBindingRequest chan *v1.CreateBindingRequest
}

func (t *testAdapterServer) ListBindings(context.Context, *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	return nil, nil
}

func (t *testAdapterServer) CreateBinding(c context.Context, r *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	t.ActualCreateBindingRequest <- r

	return &v1.CreateBindingResponse{}, nil
}

func (t *testAdapterServer) DeleteBinding(c context.Context, r *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	return nil, nil
}
