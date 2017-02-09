package app_test

import (
	"context"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("App Controller", func() {
	var (
		client v1.AdapterClient
	)

	BeforeEach(func() {
		_, adapterServiceHost := app.StartAdapter(
			app.WithHealthAddr("localhost:0"),
			app.WithControllerAddr("localhost:0"),
		)

		client = startAdapterClient(adapterServiceHost)
	})

	Describe("ListBindings()", func() {
		It("returns a list of known bindings", func() {
			resp, err := client.ListBindings(context.Background(), new(v1.ListBindingsRequest))
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.Bindings).To(HaveLen(0))
		})
	})

	Describe("CreateBinding()", func() {
		It("creates a new binding", func() {
			binding := &v1.Binding{
				AppId:    "app-guid",
				Hostname: "a-hostname",
				Drain:    "a-drain",
			}

			_, err := client.CreateBinding(context.Background(), &v1.CreateBindingRequest{
				Binding: binding,
			})
			Expect(err).ToNot(HaveOccurred())

			resp, err := client.ListBindings(context.Background(), new(v1.ListBindingsRequest))
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.Bindings).To(HaveLen(1))
		})
	})
})

func startAdapterClient(addr string) v1.AdapterClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())

	return v1.NewAdapterClient(conn)
}
