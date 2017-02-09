package app_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("App", func() {
	var (
		adapterServiceHost string
		adapterHealthAddr  string
		client             v1.AdapterClient
		binding            *v1.Binding
	)

	BeforeEach(func() {
		adapterHealthAddr, adapterServiceHost = app.StartAdapter(
			app.WithHealthAddr("localhost:0"),
			app.WithControllerAddr("localhost:0"),
		)

		client = startAdapterClient(adapterServiceHost)
		binding = &v1.Binding{
			AppId:    "app-guid",
			Hostname: "a-hostname",
			Drain:    "a-drain",
		}
	})

	It("creates a new binding", func() {
		_, err := client.CreateBinding(context.Background(), &v1.CreateBindingRequest{
			Binding: binding,
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := client.ListBindings(context.Background(), new(v1.ListBindingsRequest))
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Bindings).To(HaveLen(1))

		healthResp, err := http.Get(fmt.Sprintf("http://%s/health", adapterHealthAddr))
		Expect(err).ToNot(HaveOccurred())
		Expect(healthResp.StatusCode).To(Equal(http.StatusOK))

		body, err := ioutil.ReadAll(healthResp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(body).To(MatchJSON(`{"drainCount": 1}`))
	})
})

func startAdapterClient(addr string) v1.AdapterClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())

	return v1.NewAdapterClient(conn)
}
