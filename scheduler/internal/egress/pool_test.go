package egress_test

import (
	"net"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("Connection Pool", func() {
	var (
		testServer *testAdapterServer
		serverAddr string
		binding    *v1.Binding
	)

	BeforeEach(func() {
		binding = &v1.Binding{
			AppId:    "app-id",
			Hostname: "org.space.app",
			Drain:    "syslog://my-drain-url",
		}

		lis, err := net.Listen("tcp", "localhost:0")
		Expect(err).ToNot(HaveOccurred())

		testServer = NewTestAdapterServer()
		grpcServer := grpc.NewServer()
		v1.RegisterAdapterServer(grpcServer, testServer)
		go grpcServer.Serve(lis)

		serverAddr = lis.Addr().String()
	})

	It("returns the number of adapters", func() {
		adapters := []string{"1.2.3.4:1234"}
		p := egress.NewAdapterWriterPool(&egress.DefaultClientCreator{}, adapters, grpc.WithInsecure())

		Expect(p.Count()).To(Equal(1))
	})

	It("writes to a gRPC server", func() {
		p := egress.NewAdapterWriterPool(&egress.DefaultClientCreator{}, []string{serverAddr}, grpc.WithInsecure())

		p.Create(binding)

		Eventually(testServer.ActualCreateBindingRequest).Should(Receive(Equal(
			&v1.CreateBindingRequest{
				Binding: binding,
			},
		)))
	})

	It("makes a call to remove drain", func() {
		p := egress.NewAdapterWriterPool(&egress.DefaultClientCreator{}, []string{serverAddr}, grpc.WithInsecure())

		p.Delete(binding)

		Eventually(testServer.ActualDeleteBindingRequest).Should(Receive(Equal(
			&v1.DeleteBindingRequest{
				Binding: binding,
			},
		)))
	})

	It("gets a list of bindings from all adapters", func() {
		p := egress.NewAdapterWriterPool(&egress.DefaultClientCreator{}, []string{serverAddr}, grpc.WithInsecure())
		p.Create(binding)

		bindings, err := p.List()
		Expect(err).ToNot(HaveOccurred())

		Expect(bindings).To(Equal([][]*v1.Binding{
			{binding},
		}))
	})
})
