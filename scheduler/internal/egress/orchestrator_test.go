package egress_test

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	Context("Run", func() {
		It("writes syslog bindings to the writer", func() {
			reader := &SpyReader{
				drains: ingress.AppBindings{
					"app-id": ingress.Binding{
						Hostname: "org.space.app",
						Drains:   []string{"syslog://my-app-drain"},
					},
				},
			}
			client := &SpyClient{
				listBindingsResponse_: &v1.ListBindingsResponse{},
			}

			o := egress.NewOrchestrator(reader, egress.AdapterPool{client})
			go o.Run(1 * time.Millisecond)
			defer o.Stop()

			Eventually(client.createBindingRequest, 2).Should(Equal(
				&v1.CreateBindingRequest{
					&v1.Binding{
						AppId:    "app-id",
						Hostname: "org.space.app",
						Drain:    "syslog://my-app-drain",
					},
				},
			))
		})

		It("does not write when the read fails", func() {
			reader := &SpyReader{
				err: errors.New("Nope!"),
			}
			client := &SpyClient{}

			o := egress.NewOrchestrator(reader, egress.AdapterPool{client})
			go o.Run(1 * time.Millisecond)
			defer o.Stop()

			Consistently(client.createBindingRequest).Should(BeNil())
		})

		It("deletes bindings that are no longer present", func() {
			reader := &SpyReader{
				drains: ingress.AppBindings{
					"app-id": ingress.Binding{
						Hostname: "org.space.app",
						Drains:   []string{"syslog://my-app-drain"},
					},
				},
			}
			client := &SpyClient{
				listBindingsResponse_: &v1.ListBindingsResponse{
					Bindings: []*v1.Binding{
						&v1.Binding{
							AppId:    "app-id",
							Hostname: "org.space.app",
							Drain:    "syslog://my-app-drain",
						},
						&v1.Binding{
							AppId:    "app-id",
							Hostname: "org.space.app",
							Drain:    "syslog://other-drain",
						},
					},
				},
			}

			o := egress.NewOrchestrator(reader, egress.AdapterPool{client})
			go o.Run(1 * time.Millisecond)
			defer o.Stop()

			Eventually(client.deleteBindingRequest, 2).Should(Equal(
				&v1.DeleteBindingRequest{
					&v1.Binding{
						AppId:    "app-id",
						Hostname: "org.space.app",
						Drain:    "syslog://other-drain",
					},
				},
			))
		})
	})

	Context("Bindings", func() {
		binding := &v1.Binding{
			AppId:    "app-id",
			Hostname: "org.space.app",
			Drain:    "syslog://my-drain-url",
		}

		It("returns the number of adapters", func() {
			reader := &SpyReader{}
			client := &SpyClient{}

			o := egress.NewOrchestrator(reader, egress.AdapterPool{client})

			Expect(o.Count()).To(Equal(1))
		})

		It("makes a call to remove drain", func() {
			reader := &SpyReader{}
			client := &SpyClient{}
			toDelete := &v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://my-drain-url",
			}
			actual := [][]*v1.Binding{{binding}}
			expected := ingress.AppBindings{}

			o := egress.NewOrchestrator(reader, egress.AdapterPool{client})

			o.DeleteAll(actual, expected)

			Expect(client.deleteBindingRequest()).To(Equal(
				&v1.DeleteBindingRequest{Binding: toDelete},
			))
		})

		Context("List", func() {
			It("gets a list of bindings from all adapters", func() {
				client := &SpyClient{}
				client.listBindingsResponse_ = &v1.ListBindingsResponse{
					Bindings: []*v1.Binding{binding},
				}
				reader := &SpyReader{}

				o := egress.NewOrchestrator(reader, egress.AdapterPool{client})

				bindings, err := o.List()

				Expect(client.listCalled()).To(Equal(true))
				Expect(err).ToNot(HaveOccurred())
				Expect(len(bindings)).To(Equal(1))
				Expect(len(bindings[0])).To(Equal(1))
				Expect(bindings[0][0]).To(Equal(binding))
			})

			It("adds an empty slice when list fails", func() {
				client := &SpyClient{}
				client.listBindingsError_ = errors.New("list failed")
				reader := &SpyReader{}

				o := egress.NewOrchestrator(reader, egress.AdapterPool{client})

				bindings, _ := o.List()
				Expect(len(bindings)).To(Equal(1))
				Expect(len(bindings[0])).To(Equal(0))
			})
		})

		Context("Create", func() {
			appBinding := ingress.Binding{
				Drains:   []string{"syslog://my-drain-url"},
				Hostname: "org.space.app",
			}

			It("writes to a gRPC server with a single client", func() {
				client := &SpyClient{}
				reader := &SpyReader{}
				o := egress.NewOrchestrator(reader, egress.AdapterPool{client})

				o.Create([][]*v1.Binding{}, ingress.AppBindings{"app-id": appBinding})

				Expect(client.createCalled()).To(Equal(true))
				Expect(client.createBindingRequest()).To(Equal(
					&v1.CreateBindingRequest{Binding: binding},
				))
			})

			It("writes both gRPC servers with two clients", func() {
				reader := &SpyReader{}
				firstClient := &SpyClient{}
				secondClient := &SpyClient{}
				o := egress.NewOrchestrator(reader, egress.AdapterPool{firstClient, secondClient})

				o.Create([][]*v1.Binding{}, ingress.AppBindings{"app-id": appBinding})

				Expect(firstClient.createCalled()).To(Equal(true))
				Expect(secondClient.createCalled()).To(Equal(true))
			})

			It("writes only to two gRPC servers with many clients", func() {
				reader := &SpyReader{}
				clients := egress.AdapterPool{&SpyClient{}, &SpyClient{}, &SpyClient{}}
				o := egress.NewOrchestrator(reader, clients)

				o.Create([][]*v1.Binding{}, ingress.AppBindings{"app-id": appBinding})

				createCalled := 0
				for _, client := range clients {
					if (client.(*SpyClient)).createCalled() {
						createCalled++
					}
				}
				Expect(createCalled).To(Equal(2))
			})

			It("writes to only one gRPC server when another already has the binding", func() {
				reader := &SpyReader{}
				clients := egress.AdapterPool{&SpyClient{}, &SpyClient{}, &SpyClient{}}
				o := egress.NewOrchestrator(reader, clients)

				o.Create([][]*v1.Binding{
					{&v1.Binding{"app-id", "org.space.app", "syslog://my-drain-url"}},
				}, ingress.AppBindings{"app-id": appBinding})

				createCalled := 0
				for _, client := range clients {
					if (client.(*SpyClient)).createCalled() {
						createCalled++
					}
				}
				Expect(createCalled).To(Equal(1))
			})
		})
	})
})

type SpyReader struct {
	drains ingress.AppBindings
	err    error
}

func (s *SpyReader) FetchBindings() (appBindings ingress.AppBindings, err error) {
	return s.drains, s.err
}

type SpyClient struct {
	createCalled_         bool
	createBindingRequest_ *v1.CreateBindingRequest

	deleteCalled_         bool
	deleteBindingRequest_ *v1.DeleteBindingRequest

	listCalled_           bool
	listBindingsResponse_ *v1.ListBindingsResponse
	listBindingsError_    error
	mu                    sync.RWMutex
}

func (s *SpyClient) createCalled() bool {
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
	s.createCalled_ = true
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
