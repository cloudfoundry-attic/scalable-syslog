package egress

import (
	"log"

	"golang.org/x/net/context"

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

func (p AdapterPool) List(ctx context.Context, adapter interface{}) ([]interface{}, error) {
	results, err := adapter.(v1.AdapterClient).ListBindings(ctx, &v1.ListBindingsRequest{})
	if err != nil {
		return nil, err
	}

	var bindings []interface{}
	for _, b := range results.Bindings {
		bindings = append(bindings, b)
	}

	return bindings, nil
}

func (p AdapterPool) Add(ctx context.Context, adapter, task interface{}) error {
	_, err := adapter.(v1.AdapterClient).CreateBinding(ctx, &v1.CreateBindingRequest{
		Binding: task.(*v1.Binding),
	})

	return err
}

func (p AdapterPool) Remove(ctx context.Context, adapter, task interface{}) error {
	_, err := adapter.(v1.AdapterClient).DeleteBinding(ctx, &v1.DeleteBindingRequest{
		Binding: task.(*v1.Binding),
	})

	return err
}
