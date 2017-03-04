package egress_test

import (
	"net"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DefaultClientCreator", func() {
	It("returns an adapter client", func() {
		addr, cleanup := startGRPCServer()
		defer cleanup()
		creator := &egress.DefaultClientCreator{}

		client, _ := creator.Create(addr, grpc.WithInsecure())

		_, err := client.ListBindings(context.Background(), &v1.ListBindingsRequest{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns an error when a server is not running", func() {
		creator := &egress.DefaultClientCreator{}

		client, _ := creator.Create("0.0.0.0:1234", grpc.WithInsecure())

		_, err := client.ListBindings(context.Background(), &v1.ListBindingsRequest{})
		Expect(err).To(HaveOccurred())
	})
})

func startGRPCServer() (string, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	Expect(err).NotTo(HaveOccurred())
	testServer := NewTestAdapterServer()
	grpcServer := grpc.NewServer()
	v1.RegisterAdapterServer(grpcServer, testServer)

	go grpcServer.Serve(lis)

	return lis.Addr().String(), func() {
		grpcServer.Stop()
		lis.Close()
	}
}
