package egress_test

import (
	"errors"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AdapterService", func() {
	Describe("List", func() {
		It("gets a list of de-duped bindings from all adapters", func() {
			client := &SpyClient{}
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
			client.listBindingsResponse = &v1.ListBindingsResponse{
				Bindings: []*v1.Binding{binding, duplicate},
			}

			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client})

			state := s.List()

			Expect(client.listCalled).To(Equal(true))
			Expect(len(state)).To(Equal(1))
			Expect(state["test-addr"][0]).To(Equal(*binding))
		})

		It("returns no bindings when list fails", func() {
			client := &SpyClient{}
			client.listBindingsErr = errors.New("list failed")

			s := egress.NewAdapterService(egress.AdapterPool{"": client})

			bindings := s.List()
			Expect(len(bindings)).To(Equal(0))
		})
	})

	Describe("Transition", func() {
		It("does nothing if both states are the same", func() {
			actual := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := actual

			client := &SpyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client})
			s.Transition(actual, desired)
			Expect(client.createRequests).To(BeEmpty())
			Expect(client.deleteRequests).To(BeEmpty())
		})

		It("adds bindings for new adapters", func() {
			actual := egress.State{}
			desired := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}

			client := &SpyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client})
			s.Transition(actual, desired)

			Expect(client.createRequests).To(ConsistOf(
				&v1.CreateBindingRequest{
					Binding: &v1.Binding{
						AppId:    "app-id",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			))
			Expect(client.deleteRequests).To(BeEmpty())
		})

		It("deletes bindings for adapters that no longer exist", func() {
			actual := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := egress.State{}

			client := &SpyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client})
			s.Transition(actual, desired)

			Expect(client.createRequests).To(BeEmpty())
			Expect(client.deleteRequests).To(ConsistOf(
				&v1.DeleteBindingRequest{
					Binding: &v1.Binding{
						AppId:    "app-id",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			))
		})

		It("adds new bindings for existing adapters", func() {
			actual := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
					{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}

			client := &SpyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client})
			s.Transition(actual, desired)

			Expect(client.createRequests).To(ConsistOf(
				&v1.CreateBindingRequest{
					Binding: &v1.Binding{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			))
			Expect(client.deleteRequests).To(BeEmpty())
		})

		It("deletes old bindings for existing adapters", func() {
			actual := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
					{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}

			client := &SpyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client})
			s.Transition(actual, desired)

			Expect(client.createRequests).To(BeEmpty())
			Expect(client.deleteRequests).To(ConsistOf(
				&v1.DeleteBindingRequest{
					Binding: &v1.Binding{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			))
		})

		It("can move a binding from one adapter to another", func() {
			actual := egress.State{
				"test-addr-1": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
					{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
				"test-addr-2": []v1.Binding{
					{
						AppId:    "app-id-3",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := egress.State{
				"test-addr-1": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
				"test-addr-2": []v1.Binding{
					{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
					{
						AppId:    "app-id-3",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}

			client1 := &SpyClient{}
			client2 := &SpyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{
				"test-addr-1": client1,
				"test-addr-2": client2,
			})
			s.Transition(actual, desired)

			Expect(client2.createRequests).To(ConsistOf(
				&v1.CreateBindingRequest{
					Binding: &v1.Binding{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			))
			Expect(client1.deleteRequests).To(ConsistOf(
				&v1.DeleteBindingRequest{
					Binding: &v1.Binding{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			))
		})
	})
})

type SpyClient struct {
	createRequests []*v1.CreateBindingRequest
	deleteRequests []*v1.DeleteBindingRequest

	listCalled           bool
	listBindingsResponse *v1.ListBindingsResponse
	listBindingsErr      error
}

func (s *SpyClient) CreateBinding(
	ctx context.Context,
	in *v1.CreateBindingRequest,
	opts ...grpc.CallOption,
) (*v1.CreateBindingResponse, error) {
	s.createRequests = append(s.createRequests, in)
	return nil, nil
}

func (s *SpyClient) DeleteBinding(
	ctx context.Context,
	in *v1.DeleteBindingRequest,
	opts ...grpc.CallOption,
) (*v1.DeleteBindingResponse, error) {
	s.deleteRequests = append(s.deleteRequests, in)
	return nil, nil
}

func (s *SpyClient) ListBindings(
	ctx context.Context,
	in *v1.ListBindingsRequest,
	opts ...grpc.CallOption,
) (*v1.ListBindingsResponse, error) {
	s.listCalled = true
	return s.listBindingsResponse, s.listBindingsErr
}
