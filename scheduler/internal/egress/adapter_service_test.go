package egress_test

import (
	"errors"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DefaultAdapterService", func() {
	binding := v1.Binding{
		AppId:    "app-id",
		Hostname: "org.space.app",
		Drain:    "syslog://my-drain-url",
	}

	It("returns the number of adapters", func() {
		client := &SpyClient{}

		s := egress.NewAdapterService(egress.AdapterPool{client})

		Expect(s.Count()).To(Equal(1))
	})

	It("makes a call to remove drain", func() {
		client := &SpyClient{}
		s := egress.NewAdapterService(egress.AdapterPool{client})

		actual := ingress.Bindings{binding}
		expected := ingress.Bindings{}
		s.DeleteDelta(actual, expected)

		expectedToBeDeleted := &v1.Binding{
			AppId:    "app-id",
			Hostname: "org.space.app",
			Drain:    "syslog://my-drain-url",
		}
		Expect(client.deleteBindingRequest()).To(Equal(
			&v1.DeleteBindingRequest{Binding: expectedToBeDeleted},
		))
	})

	It("remove drains on hostname rename", func() {
		client := &SpyClient{}
		s := egress.NewAdapterService(egress.AdapterPool{client})

		actual := ingress.Bindings{binding}
		expected := ingress.Bindings{
			v1.Binding{AppId: "app-id", Hostname: "org.space.other-app", Drain: "syslog://my-drain-url"},
		}
		s.DeleteDelta(actual, expected)

		expectedToBeDeleted := &v1.Binding{
			AppId:    "app-id",
			Hostname: "org.space.app",
			Drain:    "syslog://my-drain-url",
		}
		Expect(client.deleteBindingRequest()).To(Equal(
			&v1.DeleteBindingRequest{Binding: expectedToBeDeleted},
		))
	})

	Context("List", func() {
		It("gets a list of bindings from all adapters", func() {
			client := &SpyClient{}
			client.listBindingsResponse_ = &v1.ListBindingsResponse{
				Bindings: []*v1.Binding{
					{
						AppId:    binding.AppId,
						Hostname: binding.Hostname,
						Drain:    binding.Drain,
					},
				},
			}

			s := egress.NewAdapterService(egress.AdapterPool{client})

			bindings, err := s.List()

			Expect(client.listCalled()).To(Equal(true))
			Expect(err).ToNot(HaveOccurred())
			Expect(len(bindings)).To(Equal(1))
			Expect(bindings[0]).To(Equal(binding))
		})

		It("returns no bindings when list fails", func() {
			client := &SpyClient{}
			client.listBindingsError_ = errors.New("list failed")

			s := egress.NewAdapterService(egress.AdapterPool{client})

			bindings, _ := s.List()
			Expect(len(bindings)).To(Equal(0))
		})
	})

	Context("Create", func() {
		appBinding := v1.Binding{
			AppId:    "app-id",
			Drain:    "syslog://my-drain-url",
			Hostname: "org.space.app",
		}

		Context("with a single client in the pool", func() {
			It("creates the binding on the client", func() {
				client := &SpyClient{}
				s := egress.NewAdapterService(egress.AdapterPool{client})

				s.CreateDelta(ingress.Bindings{}, ingress.Bindings{appBinding})

				Expect(client.createCalled()).To(Equal(1))
				Expect(client.createBindingRequest()).To(Equal(
					&v1.CreateBindingRequest{Binding: &v1.Binding{
						AppId:    binding.AppId,
						Hostname: binding.Hostname,
						Drain:    binding.Drain,
					}},
				))
			})

			It("does not create a second binding when the client already has the binding", func() {
				client := &SpyClient{}
				s := egress.NewAdapterService(egress.AdapterPool{client})

				actual := ingress.Bindings{
					{
						AppId:    "app-id",
						Hostname: "org.space.app",
						Drain:    "syslog://my-drain-url",
					},
				}
				expected := ingress.Bindings{appBinding}
				s.CreateDelta(actual, expected)

				Expect(client.createCalled()).To(Equal(0))
			})
		})

		It("writes both gRPC servers with two clients", func() {
			firstClient := &SpyClient{}
			secondClient := &SpyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{firstClient, secondClient})

			s.CreateDelta(ingress.Bindings{}, ingress.Bindings{appBinding})

			Expect(firstClient.createCalled()).To(Equal(1))
			Expect(secondClient.createCalled()).To(Equal(1))
		})

		It("writes only to two gRPC servers with many clients", func() {
			clients := egress.AdapterPool{&SpyClient{}, &SpyClient{}, &SpyClient{}}
			s := egress.NewAdapterService(clients)

			s.CreateDelta(ingress.Bindings{}, ingress.Bindings{appBinding})

			createCalled := 0
			for _, client := range clients {
				if (client.(*SpyClient)).createCalled() > 0 {
					createCalled++
				}
			}
			Expect(createCalled).To(Equal(2))
		})

		It("writes to only one gRPC server when another already has the binding", func() {
			clients := egress.AdapterPool{&SpyClient{}, &SpyClient{}, &SpyClient{}}
			s := egress.NewAdapterService(clients)

			s.CreateDelta(ingress.Bindings{
				{AppId: "app-id", Hostname: "org.space.app", Drain: "syslog://my-drain-url"},
			}, ingress.Bindings{appBinding})

			createCalled := 0
			for _, client := range clients {
				if (client.(*SpyClient)).createCalled() > 0 {
					createCalled++
				}
			}
			Expect(createCalled).To(Equal(1))
		})

		It("writes to two adapters for each drain binding only once", func() {
			appBindings := ingress.Bindings{
				v1.Binding{AppId: "app-id", Drain: "syslog://my-drain-url", Hostname: "org.space.app"},
				v1.Binding{AppId: "app-id", Drain: "syslog://another-drain", Hostname: "org.space.app"},
			}

			clients := egress.AdapterPool{&SpyClient{}, &SpyClient{}}
			s := egress.NewAdapterService(clients)

			s.CreateDelta(
				ingress.Bindings{},
				appBindings,
			)

			createCalled := 0
			for _, client := range clients {
				createCalled += (client.(*SpyClient)).createCalled()
			}
			Expect(createCalled).To(Equal(4))

			s.CreateDelta(
				ingress.Bindings{
					{AppId: "app-id", Hostname: "org.space.app", Drain: "syslog://my-drain-url"},
					{AppId: "app-id", Hostname: "org.space.app", Drain: "syslog://another-drain"},
					{AppId: "app-id", Hostname: "org.space.app", Drain: "syslog://my-drain-url"},
					{AppId: "app-id", Hostname: "org.space.app", Drain: "syslog://another-drain"},
				},
				appBindings,
			)

			createCalled = 0
			for _, client := range clients {
				createCalled += (client.(*SpyClient)).createCalled()
			}
			Expect(createCalled).To(Equal(4))
		})

		It("creates a new drain on hostname rename", func() {
			client := &SpyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{client})

			actual := ingress.Bindings{binding}
			expected := ingress.Bindings{
				v1.Binding{AppId: "app-id", Drain: "syslog://my-drain-url", Hostname: "org.space.other-app"},
			}
			s.CreateDelta(actual, expected)

			expectedToBeCreated := &v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.other-app",
				Drain:    "syslog://my-drain-url",
			}
			Expect(client.createBindingRequest()).To(Equal(
				&v1.CreateBindingRequest{Binding: expectedToBeCreated},
			))
		})
	})
})

type SpyClient struct {
	createCalled_         int
	createBindingRequest_ *v1.CreateBindingRequest

	deleteCalled_         bool
	deleteBindingRequest_ *v1.DeleteBindingRequest

	listCalled_           bool
	listBindingsResponse_ *v1.ListBindingsResponse
	listBindingsError_    error
	mu                    sync.RWMutex
}

func (s *SpyClient) createCalled() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createCalled_
}

func (s *SpyClient) createBindingRequest() *v1.CreateBindingRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createBindingRequest_
}

func (s *SpyClient) deleteCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteCalled_
}

func (s *SpyClient) deleteBindingRequest() *v1.DeleteBindingRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteBindingRequest_
}

func (s *SpyClient) listCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listCalled_
}

func (s *SpyClient) CreateBinding(
	ctx context.Context,
	in *v1.CreateBindingRequest,
	opts ...grpc.CallOption,
) (*v1.CreateBindingResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createCalled_++
	s.createBindingRequest_ = in
	return nil, nil
}

func (s *SpyClient) DeleteBinding(
	ctx context.Context,
	in *v1.DeleteBindingRequest,
	opts ...grpc.CallOption,
) (*v1.DeleteBindingResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteCalled_ = true
	s.deleteBindingRequest_ = in
	return nil, nil
}

func (s *SpyClient) ListBindings(
	ctx context.Context,
	in *v1.ListBindingsRequest,
	opts ...grpc.CallOption,
) (*v1.ListBindingsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalled_ = true
	return s.listBindingsResponse_, s.listBindingsError_
}
