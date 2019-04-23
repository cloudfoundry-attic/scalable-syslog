package egress_test

import (
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AdapterPool", func() {
	spyMetricClient := testhelper.NewMetricClient()

	It("returns a Pool of adapter clients", func() {
		addr, cleanup := startGRPCServer()
		defer cleanup()

		adapterPool := egress.NewAdapterPool([]string{addr}, nil, spyMetricClient, grpc.WithInsecure())
		pool := adapterPool.Pool
		Expect(pool).To(HaveLen(1))
		client := pool[addr]

		_, err := client.ListBindings(context.Background(), &v1.ListBindingsRequest{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns a Pool with a unconnected client", func() {
		addr := "0.0.0.0:1234"
		adapterPool := egress.NewAdapterPool([]string{addr}, nil, spyMetricClient, grpc.WithInsecure())
		pool := adapterPool.Pool
		Expect(pool).To(HaveLen(1))
		client := pool[addr]

		_, err := client.ListBindings(context.Background(), &v1.ListBindingsRequest{})
		Expect(err).To(HaveOccurred())
	})

	It("dedupes multiple adapters with the same addr", func() {
		adapterPool := egress.NewAdapterPool([]string{
			"0.0.0.0:1234",
			"0.0.0.0:1236",
			"0.0.0.0:1235",
			"0.0.0.0:1234",
			"0.0.0.0:1235",
			"0.0.0.0:1236",
		}, nil, spyMetricClient, grpc.WithInsecure())
		pool := adapterPool.Pool
		Expect(pool).To(HaveLen(3))
	})

	It("reports adapterCount health metric", func() {
		addr, cleanup := startGRPCServer()
		defer cleanup()

		healthEmitter := &spyHealthEmitter{}
		egress.NewAdapterPool([]string{addr}, healthEmitter, spyMetricClient, grpc.WithInsecure())
		counters := healthEmitter.setCounterArg
		Expect(counters).To(HaveLen(1))
		Expect(counters["adapterCount"]).To(Equal(1))
	})

	It("Lists, adds and removes from the given adapter", func() {
		addr1, cleanup1 := startGRPCServer()
		defer cleanup1()
		addr2, cleanup2 := startGRPCServer()
		defer cleanup2()

		adapterPool := egress.NewAdapterPool([]string{addr1, addr2}, nil, spyMetricClient, grpc.WithInsecure())
		pool := adapterPool.Pool

		// Add 2 binding to adapter1 and 2 bindings to adapter2
		err := adapterPool.Add(context.Background(), pool[addr1], v1.Binding{})
		Expect(err).ToNot(HaveOccurred())
		err = adapterPool.Add(context.Background(), pool[addr1], v1.Binding{
			Hostname: "will-be-removed",
		})
		Expect(err).ToNot(HaveOccurred())
		err = adapterPool.Add(context.Background(), pool[addr2], v1.Binding{})
		Expect(err).ToNot(HaveOccurred())
		err = adapterPool.Add(context.Background(), pool[addr2], v1.Binding{})
		Expect(err).ToNot(HaveOccurred())

		// Remove 1 binding from adapter1
		err = adapterPool.Remove(context.Background(), pool[addr1], v1.Binding{
			Hostname: "will-be-removed",
		})
		Expect(err).ToNot(HaveOccurred())

		results, err := adapterPool.List(context.Background(), pool[addr1])
		Expect(err).ToNot(HaveOccurred())
		Expect(results).To(HaveLen(1))

		results, err = adapterPool.List(context.Background(), pool[addr2])
		Expect(err).ToNot(HaveOccurred())
		Expect(results).To(HaveLen(2))
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
