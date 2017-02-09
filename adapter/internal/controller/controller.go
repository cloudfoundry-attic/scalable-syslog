package controller

import (
	"golang.org/x/net/context"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

type Controller struct{}

func New() *Controller {
	return new(Controller)
}

func (s *Controller) ListBindings(context.Context, *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	return new(v1.ListBindingsResponse), nil
}
