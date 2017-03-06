package egress

import (
	"context"
	"errors"
	"math/rand"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

type BindingRepository struct {
	clients []v1.AdapterClient
}

func NewBindingRepository(clients []v1.AdapterClient) *BindingRepository {
	return &BindingRepository{
		clients: clients,
	}
}

func (p *BindingRepository) List() ([][]*v1.Binding, error) {
	request := new(v1.ListBindingsRequest)

	var bindings [][]*v1.Binding
	for _, client := range p.clients {
		// TODO handle error when ListBindings fails
		resp, _ := client.ListBindings(context.Background(), request)

		bindings = append(bindings, resp.Bindings)
	}

	return bindings, nil
}

func (p *BindingRepository) Create(b *v1.Binding) error {
	request := &v1.CreateBindingRequest{
		Binding: b,
	}

	clientLen := len(p.clients)
	switch clientLen {
	case 0:
		return errors.New("No clients to create a binding against")
	case 1:
		client := p.clients[0]
		client.CreateBinding(context.Background(), request)
	case 2:
		for _, client := range p.clients {
			client.CreateBinding(context.Background(), request)
		}
	default:
		c1Index := rand.Intn(clientLen)
		c2Index := rand.Intn(clientLen)
		c1 := p.clients[c1Index]
		c2 := p.clients[c2Index]

		c1.CreateBinding(context.Background(), request)
		c2.CreateBinding(context.Background(), request)
	}

	return nil
}

func (p *BindingRepository) Delete(b *v1.Binding) error {
	request := &v1.DeleteBindingRequest{
		Binding: b,
	}

	for _, client := range p.clients {
		client.DeleteBinding(context.Background(), request)
	}
	return nil
}

func (p *BindingRepository) Count() int {
	return len(p.clients)
}
