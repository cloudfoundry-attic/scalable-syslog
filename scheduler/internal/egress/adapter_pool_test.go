package egress_test

import (
	"net"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"

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

	Context("Subset", func() {
		addr := "1.1.1.1"

		It("returns on client when one is in the pool", func() {
			pool := egress.NewAdapterPool([]string{addr}, grpc.WithInsecure())
			subset := pool.Subset(0, 1)

			Expect(len(subset)).To(Equal(1))
		})

		It("returns two clients when two are in the pool", func() {
			pool := egress.NewAdapterPool([]string{addr, addr}, grpc.WithInsecure())
			subset := pool.Subset(0, 2)

			Expect(len(subset)).To(Equal(2))
		})

		It("returns a subset of the pool when there are many", func() {
			pool := egress.NewAdapterPool([]string{addr, addr, addr, addr}, grpc.WithInsecure())
			subset := pool.Subset(1, 2)

			Expect(len(subset)).To(Equal(2))
		})

		It("wraps back to the first index on overflow", func() {
			pool := egress.NewAdapterPool([]string{addr, addr, addr}, grpc.WithInsecure())
			subset := pool.Subset(1, 3)

			Expect(len(subset)).To(Equal(3))
		})

		It("returns all clients when more are requested than available", func() {
			pool := egress.NewAdapterPool([]string{addr, addr}, grpc.WithInsecure())
			subset := pool.Subset(0, 3)

			Expect(len(subset)).To(Equal(2))
		})
	})
})

func startGRPCServer() (string, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	Expect(err).NotTo(HaveOccurred())
	testServer := newSpyAdapterServer()
	grpcServer := grpc.NewServer()
	v1.RegisterAdapterServer(grpcServer, testServer)

	go grpcServer.Serve(lis)

	return lis.Addr().String(), func() {
		grpcServer.Stop()
		lis.Close()
	}
}
