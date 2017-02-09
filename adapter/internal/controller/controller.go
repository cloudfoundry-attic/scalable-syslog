package controller

import (
	"golang.org/x/net/context"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

type Controller struct{}

func New() *Controller {
	return new(Controller)
}

func (s *Controller) Drains(context.Context, *v1.DrainsRequest) (*v1.DrainsResponse, error) {
	return new(v1.DrainsResponse), nil
}
