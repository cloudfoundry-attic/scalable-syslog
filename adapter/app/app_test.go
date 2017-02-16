package app_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
	"github.com/cloudfoundry-incubator/scalable-syslog/api"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("App", func() {
	var (
		adapterServiceHost string
		adapterHealthAddr  string
		client             v1.AdapterClient
		binding            *v1.Binding
		egressServer       *MockEgressServer
	)

	BeforeEach(func() {
		var logsAPIAddr string
		egressServer, logsAPIAddr = startLogsAPIServer()

		tlsConfig, err := api.NewMutualTLSConfig(
			Cert("adapter-rlp.crt"),
			Cert("adapter-rlp.key"),
			Cert("loggregator-ca.crt"),
			"fake-log-provider",
		)
		Expect(err).ToNot(HaveOccurred())

		adapterHealthAddr, adapterServiceHost = app.StartAdapter(
			app.WithHealthAddr("localhost:0"),
			app.WithControllerAddr("localhost:0"),
			app.WithLogsEgressAPIAddr(logsAPIAddr),
			app.WithLogsEgressAPIConnCount(5),
			app.WithLogsEgressAPITLSConfig(tlsConfig),
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

	It("deletes a binding", func() {
		_, err := client.CreateBinding(context.Background(), &v1.CreateBindingRequest{
			Binding: binding,
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = client.DeleteBinding(context.Background(), &v1.DeleteBindingRequest{
			Binding: binding,
		})
		Expect(err).ToNot(HaveOccurred())

		healthResp, err := http.Get(fmt.Sprintf("http://%s/health", adapterHealthAddr))
		Expect(err).ToNot(HaveOccurred())
		Expect(healthResp.StatusCode).To(Equal(http.StatusOK))

		body, err := ioutil.ReadAll(healthResp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(body).To(MatchJSON(`{"drainCount": 0}`))
	})

	It("connects the logs egress API", func() {
		_, err := client.CreateBinding(context.Background(), &v1.CreateBindingRequest{
			Binding: binding,
		})
		Expect(err).ToNot(HaveOccurred())

		Eventually(egressServer.receiver).Should(HaveLen(5))
	})
})

func startAdapterClient(addr string) v1.AdapterClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())

	return v1.NewAdapterClient(conn)
}

func startLogsAPIServer() (*MockEgressServer, string) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	tlsConfig, err := api.NewMutualTLSConfig(
		Cert("fake-log-provider.crt"),
		Cert("fake-log-provider.key"),
		Cert("loggregator-ca.crt"),
		"fake-log-provider",
	)
	Expect(err).ToNot(HaveOccurred())

	mockEgressServer := NewMockEgressServer()
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	v2.RegisterEgressServer(grpcServer, mockEgressServer)

	go func() {
		log.Fatalf("failed to serve: %v", grpcServer.Serve(lis))
	}()

	return mockEgressServer, lis.Addr().String()
}

type MockEgressServer struct {
	receiver chan v2.Egress_ReceiverServer
}

func NewMockEgressServer() *MockEgressServer {
	return &MockEgressServer{
		receiver: make(chan v2.Egress_ReceiverServer, 5),
	}
}

func (m *MockEgressServer) Receiver(req *v2.EgressRequest, receiver v2.Egress_ReceiverServer) error {
	m.receiver <- receiver
	return nil
}
