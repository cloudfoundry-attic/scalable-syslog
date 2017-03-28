package binding_test

import (
	"context"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/binding"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AdapterServer", func() {
	var (
		store         *SpyStore
		healthEmitter *SpyHealthEmitter
	)

	BeforeEach(func() {
		healthEmitter = &SpyHealthEmitter{}
	})

	It("returns a list of known bindings", func() {
		store = &SpyStore{list: []*v1.Binding{nil, nil}}
		adapterServer := binding.NewAdapterServer(store, healthEmitter)

		resp, err := adapterServer.ListBindings(
			context.Background(),
			&v1.ListBindingsRequest{},
		)

		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Bindings).To(HaveLen(2))
	})

	It("adds new binding", func() {
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

	It("deletes existing bindings", func() {
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
})

type SpyHealthEmitter struct{}

func (s *SpyHealthEmitter) SetCounter(_ map[string]int) {}

type SpyStore struct {
	list   []*v1.Binding
	add    *v1.Binding
	delete *v1.Binding
}

func (s *SpyStore) Add(binding *v1.Binding) {
	s.add = binding
}
func (s *SpyStore) Delete(binding *v1.Binding) {
	s.delete = binding
}
func (s *SpyStore) List() []*v1.Binding {
	return s.list
}
