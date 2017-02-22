package egress

import (
	"context"
	"log"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	"google.golang.org/grpc"
)

type Pool struct {
	clients []v1.AdapterClient
}

func NewAdapterWriterPool(addrs []string, dialOpts ...grpc.DialOption) *Pool {
	var clients []v1.AdapterClient

	for _, a := range addrs {
		conn, err := grpc.Dial(a, dialOpts...)

		if err != nil {
			log.Print(err)
		}

		client := v1.NewAdapterClient(conn)

		clients = append(clients, client)
	}

	return &Pool{
		clients: clients,
	}
}

func (p *Pool) List() ([][]*v1.Binding, error) {
	request := new(v1.ListBindingsRequest)

	var bindings [][]*v1.Binding
	for _, client := range p.clients {
		resp, _ := client.ListBindings(context.Background(), request)

		bindings = append(bindings, resp.Bindings)
	}

	return bindings, nil
}

func (p *Pool) Create(b *v1.Binding) error {
	request := &v1.CreateBindingRequest{
		Binding: b,
	}

	for _, client := range p.clients {
		client.CreateBinding(context.Background(), request)
	}
	return nil
}

func (p *Pool) Delete(b *v1.Binding) error {
	request := &v1.DeleteBindingRequest{
		Binding: b,
	}

	for _, client := range p.clients {
		client.DeleteBinding(context.Background(), request)
	}
	return nil
}

func (p *Pool) Count() int {
	return len(p.clients)
}
