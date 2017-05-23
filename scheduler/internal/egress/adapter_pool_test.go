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
	It("returns a pool of adapter clients", func() {
		addr, cleanup := startGRPCServer()
		defer cleanup()

		pool := egress.NewAdapterPool([]string{addr}, nil, grpc.WithInsecure())
		Expect(pool).To(HaveLen(1))
		client := pool[addr]

		_, err := client.ListBindings(context.Background(), &v1.ListBindingsRequest{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns a pool with a unconnected client", func() {
		addr := "0.0.0.0:1234"
		pool := egress.NewAdapterPool([]string{addr}, nil, grpc.WithInsecure())
		Expect(pool).To(HaveLen(1))
		client := pool[addr]

		_, err := client.ListBindings(context.Background(), &v1.ListBindingsRequest{})
		Expect(err).To(HaveOccurred())
	})

	It("dedupes multiple adapters with the same addr", func() {
		pool := egress.NewAdapterPool([]string{
			"0.0.0.0:1234",
			"0.0.0.0:1236",
			"0.0.0.0:1235",
			"0.0.0.0:1234",
			"0.0.0.0:1235",
			"0.0.0.0:1236",
		}, nil, grpc.WithInsecure())
		Expect(pool).To(HaveLen(3))
	})

	It("reports adapterCount health metric", func() {
		addr, cleanup := startGRPCServer()
		defer cleanup()

		healthEmitter := &spyHealthEmitter{}
		egress.NewAdapterPool([]string{addr}, healthEmitter, grpc.WithInsecure())
		counters := healthEmitter.setCounterArg
		Expect(counters).To(HaveLen(1))
		Expect(counters["adapterCount"]).To(Equal(1))
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

type spyHealthEmitter struct {
	setCounterArg map[string]int
}

func (s *spyHealthEmitter) SetCounter(m map[string]int) {
	s.setCounterArg = m
}
