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

func (a AdapterPool) Subset(index, count int) AdapterPool {
	var pool AdapterPool

	if len(a) < count {
		return a
	}

	if index+count >= len(a) {
		missing := (index + count) - len(a)

		pool = a[index:]
		return append(pool, a[0:missing]...)
	}

	return a[index : index+count]
}
