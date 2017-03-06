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

var _ = Describe("AdapterPool", func() {
	It("returns a list of adapter clients", func() {
		addr, cleanup := startGRPCServer()
		defer cleanup()

		pool := egress.NewAdapterPool([]string{addr}, grpc.WithInsecure())
		client := pool[0]

		_, err := client.ListBindings(context.Background(), &v1.ListBindingsRequest{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns a pool with a unconnected client", func() {
		pool := egress.NewAdapterPool([]string{"0.0.0.0:1234"}, grpc.WithInsecure())
		client := pool[0]

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
