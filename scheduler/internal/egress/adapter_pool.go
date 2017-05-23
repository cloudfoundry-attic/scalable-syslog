package egress

import (
	"log"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"

	"google.golang.org/grpc"
)

type AdapterPool map[string]v1.AdapterClient

func NewAdapterPool(addrs []string, h HealthEmitter, opts ...grpc.DialOption) AdapterPool {
	pool := AdapterPool{}

	for _, addr := range addrs {
		_, ok := pool[addr]
		if ok {
			continue
		}

		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			log.Printf("error dialing adapter: %v", err)
			continue
		}
		pool[addr] = v1.NewAdapterClient(conn)
	}

	if h != nil {
		h.SetCounter(map[string]int{"adapterCount": len(pool)})
	}

	return pool
}
