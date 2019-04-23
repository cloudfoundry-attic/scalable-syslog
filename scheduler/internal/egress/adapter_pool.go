package egress

import (
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"log"

	"context"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"

	"google.golang.org/grpc"
)

type AdapterPool struct {
	badConn pulseemitter.CounterMetric
	Pool    map[string]v1.AdapterClient
}

func NewAdapterPool(addrs []string, h HealthEmitter, m MetricEmitter, opts ...grpc.DialOption) AdapterPool {
	badConnMetrics := m.NewCounterMetric("bad_adapter_connections")
	adapterPool := AdapterPool{
		badConn: badConnMetrics,
		Pool:    map[string]v1.AdapterClient{},
	}

	for _, addr := range addrs {
		_, ok := adapterPool.Pool[addr]
		if ok {
			continue
		}

		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			log.Printf("error dialing adapter: %v", err)
			badConnMetrics.Increment(uint64(1))
			continue
		}
		adapterPool.Pool[addr] = v1.NewAdapterClient(conn)
	}

	if h != nil {
		h.SetCounter(map[string]int{"adapterCount": len(adapterPool.Pool)})
	}

	return adapterPool
}

func (p AdapterPool) List(ctx context.Context, adapter interface{}) ([]interface{}, error) {
	results, err := adapter.(v1.AdapterClient).ListBindings(ctx, &v1.ListBindingsRequest{})
	if err != nil {
		log.Printf("error dialing adapter: %v", err)
		p.badConn.Increment(uint64(1))
		return nil, err
	}

	var bindings []interface{}
	for _, b := range results.Bindings {
		bindings = append(bindings, *b)
	}

	return bindings, nil
}

func (p AdapterPool) Add(ctx context.Context, adapter, task interface{}) error {
	b := task.(v1.Binding)
	_, err := adapter.(v1.AdapterClient).CreateBinding(ctx, &v1.CreateBindingRequest{
		Binding: &b,
	})

	return err
}

func (p AdapterPool) Remove(ctx context.Context, adapter, task interface{}) error {
	b := task.(v1.Binding)
	_, err := adapter.(v1.AdapterClient).DeleteBinding(ctx, &v1.DeleteBindingRequest{
		Binding: &b,
	})

	return err
}
