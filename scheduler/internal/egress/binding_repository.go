package egress

import (
	"context"
	"errors"
	"math/rand"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

type BindingRepository struct {
	pool AdapterPool
}

func NewBindingRepository(pool AdapterPool) *BindingRepository {
	return &BindingRepository{
		pool: pool,
	}
}

func (p *BindingRepository) List() ([][]*v1.Binding, error) {
	request := new(v1.ListBindingsRequest)

	var bindings [][]*v1.Binding
	for _, client := range p.pool {
		resp, err := client.ListBindings(context.Background(), request)
		if err != nil {
			bindings = append(bindings, make([]*v1.Binding, 0))
			continue
		}

		bindings = append(bindings, resp.Bindings)
	}

	return bindings, nil
}

func (p *BindingRepository) Create(b *v1.Binding) error {
	request := &v1.CreateBindingRequest{
		Binding: b,
	}

	clientLen := len(p.pool)
	switch clientLen {
	case 0:
		return errors.New("No clients to create a binding against")
	case 1:
		client := p.pool[0]
		client.CreateBinding(context.Background(), request)
	case 2:
		for _, client := range p.pool {
			client.CreateBinding(context.Background(), request)
		}
	default:
		c1Index := rand.Intn(clientLen)
		c2Index := rand.Intn(clientLen)
		c1 := p.pool[c1Index]
		c2 := p.pool[c2Index]

		c1.CreateBinding(context.Background(), request)
		c2.CreateBinding(context.Background(), request)
	}

	return nil
}

func (p *BindingRepository) Delete(b *v1.Binding) error {
	request := &v1.DeleteBindingRequest{
		Binding: b,
	}

	for _, client := range p.pool {
		client.DeleteBinding(context.Background(), request)
	}
	return nil
}

func (p *BindingRepository) Count() int {
	return len(p.pool)
}
