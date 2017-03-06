package egress_test

import (
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Binding Repository", func() {
	binding := &v1.Binding{
		AppId:    "app-id",
		Hostname: "org.space.app",
		Drain:    "syslog://my-drain-url",
	}

	It("returns the number of adapters", func() {
		p := egress.NewBindingRepository([]v1.AdapterClient{&SpyClient{}})

		Expect(p.Count()).To(Equal(1))
	})

	It("makes a call to remove drain", func() {
		spyClient := &SpyClient{}
		p := egress.NewBindingRepository([]v1.AdapterClient{spyClient})

		p.Delete(binding)

		Expect(spyClient.deleteCalled()).To(Equal(true))
		Expect(spyClient.deleteBindingRequest()).To(Equal(
			&v1.DeleteBindingRequest{Binding: binding},
		))
	})

	It("gets a list of bindings from all adapters", func() {
		spyClient := &SpyClient{}
		spyClient.listBindingsResponse_ = &v1.ListBindingsResponse{
			Bindings: []*v1.Binding{binding},
		}
		p := egress.NewBindingRepository([]v1.AdapterClient{spyClient})

		bindings, err := p.List()

		Expect(spyClient.listCalled()).To(Equal(true))
		Expect(err).ToNot(HaveOccurred())
		Expect(len(bindings)).To(Equal(1))
		Expect(len(bindings[0])).To(Equal(1))
		Expect(bindings[0][0]).To(Equal(binding))
	})

	Context("Create", func() {
		It("returns an error when there are no clients", func() {
			p := egress.NewBindingRepository([]v1.AdapterClient{})

			err := p.Create(binding)

			Expect(err).To(HaveOccurred())
		})

		It("writes to a gRPC server with a single client", func() {
			spyClient := &SpyClient{}
			p := egress.NewBindingRepository([]v1.AdapterClient{spyClient})

			p.Create(binding)

			Expect(spyClient.createCalled()).To(Equal(true))
			Expect(spyClient.createBindingRequest()).To(Equal(
				&v1.CreateBindingRequest{Binding: binding},
			))
		})

		It("writes both gRPC servers with two clients", func() {
			firstClient := &SpyClient{}
			secondClient := &SpyClient{}
			p := egress.NewBindingRepository([]v1.AdapterClient{firstClient, secondClient})

			p.Create(binding)

			Expect(firstClient.createCalled()).To(Equal(true))
			Expect(secondClient.createCalled()).To(Equal(true))
		})

		It("writes only to two gRPC servers with many clients", func() {
			clients := []v1.AdapterClient{&SpyClient{}, &SpyClient{}, &SpyClient{}}
			p := egress.NewBindingRepository(clients)

			p.Create(binding)

			createCalled := 0
			for _, client := range clients {
				if (client.(*SpyClient)).createCalled() {
					createCalled++
				}
			}
			Expect(createCalled).To(Equal(2))
		})
	})
})

type SpyClient struct {
	createCalled_         bool
	createBindingRequest_ *v1.CreateBindingRequest

	deleteCalled_         bool
	deleteBindingRequest_ *v1.DeleteBindingRequest

	listCalled_           bool
	listBindingsResponse_ *v1.ListBindingsResponse
}

func (s *SpyClient) createCalled() bool {
	return s.createCalled_
}

func (s *SpyClient) createBindingRequest() *v1.CreateBindingRequest {
	return s.createBindingRequest_
}

func (s *SpyClient) deleteCalled() bool {
	return s.deleteCalled_
}

func (s *SpyClient) deleteBindingRequest() *v1.DeleteBindingRequest {
	return s.deleteBindingRequest_
}

func (s *SpyClient) listCalled() bool {
	return s.listCalled_
}

func (s *SpyClient) CreateBinding(
	ctx context.Context,
	in *v1.CreateBindingRequest,
	opts ...grpc.CallOption,
) (*v1.CreateBindingResponse, error) {
	s.createCalled_ = true
	s.createBindingRequest_ = in
	return nil, nil
}

func (s *SpyClient) DeleteBinding(
	ctx context.Context,
	in *v1.DeleteBindingRequest,
	opts ...grpc.CallOption,
) (*v1.DeleteBindingResponse, error) {
	s.deleteCalled_ = true
	s.deleteBindingRequest_ = in
	return nil, nil
}

func (s *SpyClient) ListBindings(
	ctx context.Context,
	in *v1.ListBindingsRequest,
	opts ...grpc.CallOption,
) (*v1.ListBindingsResponse, error) {
	s.listCalled_ = true
	return s.listBindingsResponse_, nil
}
