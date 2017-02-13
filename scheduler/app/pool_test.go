package app_test

import (
	"net"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("Connection Pool", func() {
	It("returns the number of adapters", func() {
		adapters := []string{"1.2.3.4:1234"}
		p := app.NewAdapterWriterPool(adapters, grpc.WithInsecure())

		Expect(p.Count()).To(Equal(1))
	})

	It("writes to a gRPC server", func() {
		lis, err := net.Listen("tcp", "localhost:0")
		Expect(err).ToNot(HaveOccurred())

		testServer := NewTestAdapterServer()
		grpcServer := grpc.NewServer()
		v1.RegisterAdapterServer(grpcServer, testServer)
		go grpcServer.Serve(lis)

		p := app.NewAdapterWriterPool([]string{lis.Addr().String()}, grpc.WithInsecure())

		p.Write(&v1.Binding{
			AppId:    "app-id",
			Hostname: "org.space.app",
			Drain:    "syslog://my-drain-url",
		})

		Eventually(testServer.ActualCreateBindingRequest).Should(Receive(Equal(
			&v1.CreateBindingRequest{
				Binding: &v1.Binding{
					AppId:    "app-id",
					Hostname: "org.space.app",
					Drain:    "syslog://my-drain-url",
				},
			},
		)))
	})
})
