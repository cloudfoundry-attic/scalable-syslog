package service

import (
	"golang.org/x/net/context"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

type Service struct{}

func New() *Service {
	return new(Service)
}

func (s *Service) Drains(context.Context, *v1.DrainsRequest) (*v1.DrainsResponse, error) {
	return new(v1.DrainsResponse), nil
}
