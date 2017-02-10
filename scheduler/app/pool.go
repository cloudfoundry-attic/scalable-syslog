package app

import (
	"context"
	"log"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"google.golang.org/grpc"
)

type Pool struct {
	clients []v1.AdapterClient
}

func NewAdapterWriterPool(addrs []string) *Pool {
	var clients []v1.AdapterClient

	for _, a := range addrs {
		conn, err := grpc.Dial(a, grpc.WithInsecure())

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

func (p *Pool) Write(b *v1.Binding) error {
	request := &v1.CreateBindingRequest{
		Binding: b,
	}

	for _, client := range p.clients {
		client.CreateBinding(context.Background(), request)
	}
	return nil
}

func (p *Pool) Count() int {
	return len(p.clients)
}
