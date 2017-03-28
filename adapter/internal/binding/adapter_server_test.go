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
		mockStore     *mockBindingStore
		adapterServer *binding.AdapterServer
		healthEmitter *SpyHealthEmitter
	)

	BeforeEach(func() {
		mockStore = newMockBindingStore()
		healthEmitter = &SpyHealthEmitter{}
		adapterServer = binding.NewAdapterServer(mockStore, healthEmitter)
	})

	It("returns a list of known bindings", func() {
		mockStore.ListOutput.Bindings <- []*v1.Binding{nil, nil}
		resp, err := adapterServer.ListBindings(
			context.Background(),
			&v1.ListBindingsRequest{},
		)

		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Bindings).To(HaveLen(2))
	})

	It("adds new binding", func() {
		mockStore.ListOutput.Bindings <- []*v1.Binding{nil, nil}
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
		Expect(mockStore.AddInput.Binding).To(Receive(Equal(binding)))
	})

	It("deletes existing bindings", func() {
		mockStore.ListOutput.Bindings <- []*v1.Binding{nil, nil}
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
		Expect(mockStore.DeleteInput.Binding).To(Receive(Equal(binding)))
	})
})

type SpyHealthEmitter struct{}

func (s *SpyHealthEmitter) SetCounter(_ map[string]int) {}
