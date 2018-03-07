package binding_test

import (
	"context"
	"errors"

	"code.cloudfoundry.org/scalable-syslog/adapter/internal/binding"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AdapterServer", func() {
	var (
		healthEmitter *SpyHealthEmitter
	)

	BeforeEach(func() {
		healthEmitter = &SpyHealthEmitter{}
	})

	It("returns a list of known bindings", func() {
		store := &SpyStore{list: []*v1.Binding{nil, nil}}
		adapterServer := binding.NewAdapterServer(store, healthEmitter)

		resp, err := adapterServer.ListBindings(
			context.Background(),
			&v1.ListBindingsRequest{},
		)

		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Bindings).To(HaveLen(2))
	})

	It("adds new binding", func() {
		store := &SpyStore{list: []*v1.Binding{}}
		adapterServer := binding.NewAdapterServer(store, healthEmitter)
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host",
			Drain:    "some.url",
		}
		_, err := adapterServer.CreateBinding(
			context.Background(),
			&v1.CreateBindingRequest{
				Binding: binding,
			},
		)

		Expect(err).ToNot(HaveOccurred())
		Expect(store.add).To(Equal(binding))
	})

	It("returns ResourcesExhausted if it receives the max bindings exceeded error", func() {
		store := &SpyStore{
			list:     []*v1.Binding{},
			addError: binding.ErrMaxBindingsExceeded,
		}
		adapterServer := binding.NewAdapterServer(store, healthEmitter)
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host",
			Drain:    "some.url",
		}
		_, err := adapterServer.CreateBinding(
			context.Background(),
			&v1.CreateBindingRequest{
				Binding: binding,
			},
		)

		Expect(err).To(MatchError(
			grpc.Errorf(
				codes.ResourceExhausted,
				"Max bindings for adapter exceeded",
			),
		))
	})

	It("returns error if it cannot create binding", func() {
		store := &SpyStore{
			list:     []*v1.Binding{},
			addError: errors.New("some-err"),
		}
		adapterServer := binding.NewAdapterServer(store, healthEmitter)
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host",
			Drain:    "some.url",
		}
		_, err := adapterServer.CreateBinding(
			context.Background(),
			&v1.CreateBindingRequest{
				Binding: binding,
			},
		)

		Expect(err).To(MatchError(store.addError))
	})

	It("increments the drain count when creating a binding", func() {
		store := &SpyStore{list: []*v1.Binding{nil}}
		adapterServer := binding.NewAdapterServer(store, healthEmitter)
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host",
			Drain:    "some.url",
		}
		adapterServer.CreateBinding(
			context.Background(),
			&v1.CreateBindingRequest{
				Binding: binding,
			},
		)

		Expect(healthEmitter.setCounter).To(Equal(map[string]int{
			"drainCount": 1,
		}))
	})

	It("deletes existing bindings", func() {
		store := &SpyStore{list: []*v1.Binding{}}
		adapterServer := binding.NewAdapterServer(store, healthEmitter)
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host",
			Drain:    "some.url",
		}
		_, err := adapterServer.DeleteBinding(
			context.Background(),
			&v1.DeleteBindingRequest{
				Binding: binding,
			})

		Expect(err).ToNot(HaveOccurred())
		Expect(store.delete).To(Equal(binding))
	})

	It("decrements the drain count when deleting a binding", func() {
		store := &SpyStore{list: []*v1.Binding{}}
		adapterServer := binding.NewAdapterServer(store, healthEmitter)
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host",
			Drain:    "some.url",
		}
		adapterServer.CreateBinding(
			context.Background(),
			&v1.CreateBindingRequest{
				Binding: binding,
			},
		)

		Expect(healthEmitter.setCounter).To(Equal(map[string]int{
			"drainCount": 0,
		}))
	})
})

type SpyHealthEmitter struct {
	setCounter map[string]int
}

func (s *SpyHealthEmitter) SetCounter(counts map[string]int) {
	s.setCounter = counts
}

type SpyStore struct {
	list     []*v1.Binding
	add      *v1.Binding
	addError error
	delete   *v1.Binding
}

func (s *SpyStore) Add(binding *v1.Binding) error {
	s.add = binding
	return s.addError
}
func (s *SpyStore) Delete(binding *v1.Binding) {
	s.delete = binding
}
func (s *SpyStore) List() []*v1.Binding {
	return s.list
}
