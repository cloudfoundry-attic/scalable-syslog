package egress_test

import (
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection Pool", func() {
	serverAddr := "1.2.3.4:1234"
	binding := &v1.Binding{
		AppId:    "app-id",
		Hostname: "org.space.app",
		Drain:    "syslog://my-drain-url",
	}

	It("returns the number of adapters", func() {
		adapters := []string{serverAddr}
		p := egress.NewAdapterWriterPool(&SpyCreator{}, adapters)

		Expect(p.Count()).To(Equal(1))
	})

	It("creates a client connection with the passed in addr", func() {
		spyClient := &SpyAdapterClient{}
		creator := &SpyCreator{client: spyClient}

		egress.NewAdapterWriterPool(creator, []string{serverAddr})

		Expect(creator.createAddr()).To(Equal(serverAddr))
	})

	It("writes to a gRPC server", func() {
		spyClient := &SpyAdapterClient{}
		creator := &SpyCreator{client: spyClient}
		p := egress.NewAdapterWriterPool(creator, []string{serverAddr})

		p.Create(binding)

		Expect(spyClient.createCalled()).To(Equal(true))
		Expect(spyClient.createBindingRequest()).To(Equal(
			&v1.CreateBindingRequest{Binding: binding},
		))
	})

	It("makes a call to remove drain", func() {
		spyClient := &SpyAdapterClient{}
		creator := &SpyCreator{client: spyClient}
		p := egress.NewAdapterWriterPool(creator, []string{serverAddr})

		p.Delete(binding)

		Expect(spyClient.deleteCalled()).To(Equal(true))
		Expect(spyClient.deleteBindingRequest()).To(Equal(
			&v1.DeleteBindingRequest{Binding: binding},
		))
	})

	It("gets a list of bindings from all adapters", func() {
		spyClient := &SpyAdapterClient{}
		spyClient.listBindingsResponse_ = &v1.ListBindingsResponse{
			Bindings: []*v1.Binding{binding},
		}
		creator := &SpyCreator{client: spyClient}
		p := egress.NewAdapterWriterPool(creator, []string{serverAddr})

		bindings, err := p.List()

		Expect(spyClient.listCalled()).To(Equal(true))
		Expect(err).ToNot(HaveOccurred())
		Expect(len(bindings)).To(Equal(1))
		Expect(len(bindings[0])).To(Equal(1))
		Expect(bindings[0][0]).To(Equal(binding))
	})
})

type SpyCreator struct {
	client *SpyAdapterClient

	createAddr_ string
}

func (s *SpyCreator) createAddr() string {
	return s.createAddr_
}

func (s *SpyCreator) Create(addr string, opts ...grpc.DialOption) (v1.AdapterClient, error) {
	s.createAddr_ = addr
	return s.client, nil
}

type SpyAdapterClient struct {
	createCalled_         bool
	createBindingRequest_ *v1.CreateBindingRequest

	deleteCalled_         bool
	deleteBindingRequest_ *v1.DeleteBindingRequest

	listCalled_           bool
	listBindingsResponse_ *v1.ListBindingsResponse
}

func (s *SpyAdapterClient) createCalled() bool {
	return s.createCalled_
}

func (s *SpyAdapterClient) createBindingRequest() *v1.CreateBindingRequest {
	return s.createBindingRequest_
}

func (s *SpyAdapterClient) deleteCalled() bool {
	return s.deleteCalled_
}

func (s *SpyAdapterClient) deleteBindingRequest() *v1.DeleteBindingRequest {
	return s.deleteBindingRequest_
}

func (s *SpyAdapterClient) listCalled() bool {
	return s.listCalled_
}

func (s *SpyAdapterClient) CreateBinding(
	ctx context.Context,
	in *v1.CreateBindingRequest,
	opts ...grpc.CallOption,
) (*v1.CreateBindingResponse, error) {
	s.createCalled_ = true
	s.createBindingRequest_ = in
	return nil, nil
}

func (s *SpyAdapterClient) DeleteBinding(
	ctx context.Context,
	in *v1.DeleteBindingRequest,
	opts ...grpc.CallOption,
) (*v1.DeleteBindingResponse, error) {
	s.deleteCalled_ = true
	s.deleteBindingRequest_ = in
	return nil, nil
}

func (s *SpyAdapterClient) ListBindings(
	ctx context.Context,
	in *v1.ListBindingsRequest,
	opts ...grpc.CallOption,
) (*v1.ListBindingsResponse, error) {
	s.listCalled_ = true
	return s.listBindingsResponse_, nil
}
