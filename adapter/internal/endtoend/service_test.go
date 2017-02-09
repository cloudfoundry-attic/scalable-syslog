package endtoend_test

import (
	"context"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Service", func() {
	var (
		adapterServiceHost string
	)

	BeforeEach(func() {
		_, adapterServiceHost = app.StartAdapter(
			app.WithHealthAddr("localhost:0"),
			app.WithControllerAddr("localhost:0"),
		)
	})

	It("returns a list of known drains", func() {
		client := startAdapterClient(adapterServiceHost)

		drains, err := client.Drains(context.Background(), new(v1.DrainsRequest))
		Expect(err).ToNot(HaveOccurred())
		Expect(drains.Drains).To(HaveLen(0))
	})
})

func startAdapterClient(addr string) v1.AdapterClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())

	return v1.NewAdapterClient(conn)
}
