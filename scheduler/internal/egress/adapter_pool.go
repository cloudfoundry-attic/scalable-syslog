package egress

import (
	"log"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	"google.golang.org/grpc"
)

type AdapterPool []v1.AdapterClient

func NewAdapterPool(addrs []string, opts ...grpc.DialOption) AdapterPool {
	var pool AdapterPool

	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			log.Printf("error dialing adapter: %v", err)
			continue
		}

		c := v1.NewAdapterClient(conn)

		pool = append(pool, c)
	}

	return pool
}
