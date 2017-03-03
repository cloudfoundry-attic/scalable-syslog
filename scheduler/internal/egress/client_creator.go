package egress

import (
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	"google.golang.org/grpc"
)

type ClientCreator struct{}

func (*ClientCreator) Create(addr string, opts ...grpc.DialOption) (v1.AdapterClient, error) {
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return v1.NewAdapterClient(conn), nil
}
