package egress_test

import (
	"errors"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DefaultAdapterService", func() {
	var (
		binding       v1.Binding
		healthEmitter *SpyHealthEmitter
	)

	BeforeEach(func() {
		binding = v1.Binding{
			AppId:    "app-id",
			Hostname: "org.space.app",
			Drain:    "syslog://my-drain-url",
		}
		healthEmitter = &SpyHealthEmitter{}
	})

	It("makes a call to remove drain", func() {
		client := &spyClient{}
		s := egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter)

		actual := ingress.Bindings{binding}
		expected := ingress.Bindings{}
		s.DeleteDelta(actual, expected)

		expectedToBeDeleted := &v1.Binding{
			AppId:    "app-id",
			Hostname: "org.space.app",
			Drain:    "syslog://my-drain-url",
		}
		Expect(client.deleteBindingRequest).To(Equal(
			&v1.DeleteBindingRequest{Binding: expectedToBeDeleted},
		))
	})

	It("remove drains on hostname rename", func() {
		client := &spyClient{}
		s := egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter)

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
		Expect(client.deleteBindingRequest).To(Equal(
			&v1.DeleteBindingRequest{Binding: expectedToBeDeleted},
		))
	})

	Context("List", func() {
		It("gets a list of unique bindings per adapter", func() {
			clientA := &spyClient{}
			clientB := &spyClient{}
			clientC := &spyClient{}

			binding := &v1.Binding{
				AppId:    "app-id",
				Hostname: "hostname",
				Drain:    "drain",
			}
			duplicate := &v1.Binding{
				AppId:    "app-id",
				Hostname: "hostname",
				Drain:    "drain",
			}
			clientA.listBindingsResponse = &v1.ListBindingsResponse{
				Bindings: []*v1.Binding{binding, duplicate},
			}
			clientB.listBindingsResponse = &v1.ListBindingsResponse{
				Bindings: []*v1.Binding{binding, duplicate},
			}
			clientC.listBindingsResponse = &v1.ListBindingsResponse{
				Bindings: []*v1.Binding{binding, duplicate},
			}

			s := egress.NewAdapterService(
				egress.AdapterPool{clientA, clientB, clientC},
				healthEmitter,
			)

			bindings := s.List()

			Expect(clientA.listCalled).To(BeTrue())
			Expect(clientB.listCalled).To(BeTrue())
			Expect(clientC.listCalled).To(BeTrue())
			Expect(len(bindings)).To(Equal(3))
		})

		It("returns no bindings when list fails", func() {
			client := &spyClient{}
			client.listBindingsError = errors.New("list failed")

			s := egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter)

			bindings := s.List()
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
				client := &spyClient{}
				s := egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter)

				s.CreateDelta(ingress.Bindings{}, ingress.Bindings{appBinding})

				Expect(client.createCalled).To(Equal(1))
				Expect(client.createBindingRequest).To(Equal(
					&v1.CreateBindingRequest{Binding: &v1.Binding{
						AppId:    binding.AppId,
						Hostname: binding.Hostname,
						Drain:    binding.Drain,
					}},
				))
			})

			It("does not create a second binding when the client already has the binding", func() {
				client := &spyClient{}
				s := egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter)

				actual := ingress.Bindings{
					{
						AppId:    "app-id",
						Hostname: "org.space.app",
						Drain:    "syslog://my-drain-url",
					},
				}
				expected := ingress.Bindings{appBinding}
				s.CreateDelta(actual, expected)

				Expect(client.createCalled).To(Equal(0))
			})
		})

		It("writes both gRPC servers with two clients", func() {
			firstClient := &spyClient{}
			secondClient := &spyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{firstClient, secondClient}, healthEmitter)

			s.CreateDelta(ingress.Bindings{}, ingress.Bindings{appBinding})

			Expect(firstClient.createCalled).To(Equal(1))
			Expect(secondClient.createCalled).To(Equal(1))
		})

		It("writes only to two gRPC servers with many clients", func() {
			clients := egress.AdapterPool{&spyClient{}, &spyClient{}, &spyClient{}}
			s := egress.NewAdapterService(clients, healthEmitter)

			s.CreateDelta(ingress.Bindings{}, ingress.Bindings{appBinding})

			createCalled := 0
			for _, client := range clients {
				if (client.(*spyClient)).createCalled > 0 {
					createCalled++
				}
			}
			Expect(createCalled).To(Equal(2))
		})

		It("writes to only one gRPC server when another already has the binding", func() {
			clients := egress.AdapterPool{&spyClient{}, &spyClient{}, &spyClient{}}
			s := egress.NewAdapterService(clients, healthEmitter)

			s.CreateDelta(ingress.Bindings{
				{AppId: "app-id", Hostname: "org.space.app", Drain: "syslog://my-drain-url"},
			}, ingress.Bindings{appBinding})

			createCalled := 0
			for _, client := range clients {
				if (client.(*spyClient)).createCalled > 0 {
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

			clients := egress.AdapterPool{&spyClient{}, &spyClient{}}
			s := egress.NewAdapterService(clients, healthEmitter)

			s.CreateDelta(
				ingress.Bindings{},
				appBindings,
			)

			createCalled := 0
			for _, client := range clients {
				createCalled += (client.(*spyClient)).createCalled
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
				createCalled += (client.(*spyClient)).createCalled
			}
			Expect(createCalled).To(Equal(4))
		})

		It("creates a new drain on hostname rename", func() {
			client := &spyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter)

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
			Expect(client.createBindingRequest).To(Equal(
				&v1.CreateBindingRequest{Binding: expectedToBeCreated},
			))
		})
	})
})

type spyClient struct {
	createCalled         int
	createBindingRequest *v1.CreateBindingRequest

	deleteBindingRequest *v1.DeleteBindingRequest

	listCalled           bool
	listBindingsResponse *v1.ListBindingsResponse
	listBindingsError    error
}

func (s *spyClient) CreateBinding(
	ctx context.Context,
	in *v1.CreateBindingRequest,
	opts ...grpc.CallOption,
) (*v1.CreateBindingResponse, error) {
	s.createCalled++
	s.createBindingRequest = in
	return nil, nil
}

func (s *spyClient) DeleteBinding(
	ctx context.Context,
	in *v1.DeleteBindingRequest,
	opts ...grpc.CallOption,
) (*v1.DeleteBindingResponse, error) {
	s.deleteBindingRequest = in
	return nil, nil
}

func (s *spyClient) ListBindings(
	ctx context.Context,
	in *v1.ListBindingsRequest,
	opts ...grpc.CallOption,
) (*v1.ListBindingsResponse, error) {
	s.listCalled = true
	return s.listBindingsResponse, s.listBindingsError
}
