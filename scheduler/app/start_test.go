package app_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cloudfoundry-incubator/scalable-syslog/api"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Scheduler - End to End", func() {
	var (
		schedulerAddr string
		dataSource    *httptest.Server
		testServer    *testAdapterServer
	)

	BeforeEach(func() {
		dataSource = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`
					{
					  "results": {
						"9be15160-4845-4f05-b089-40e827ba61f1": {
						  "drains": [
							"syslog://some.other.url"
						  ],
						  "hostname": "org.space.logspinner"
						}
					  }
					}
				`))
		}))

		lis, err := net.Listen("tcp", "localhost:0")
		Expect(err).ToNot(HaveOccurred())

		adapterTLSConfig, err := api.NewMutualTLSConfig(
			Cert("adapter.crt"),
			Cert("adapter.key"),
			Cert("scalable-syslog-ca.crt"),
			"adapter",
		)
		if err != nil {
			log.Fatalf("Invalid TLS config: %s", err)
		}

		testServer = NewTestAdapterServer()
		grpcServer := grpc.NewServer(
			grpc.Creds(credentials.NewTLS(adapterTLSConfig)),
		)
		v1.RegisterAdapterServer(grpcServer, testServer)

		go grpcServer.Serve(lis)

		tlsConfig, err := api.NewMutualTLSConfig(
			Cert("scheduler.crt"),
			Cert("scheduler.key"),
			Cert("scalable-syslog-ca.crt"),
			"adapter",
		)
		if err != nil {
			log.Fatalf("Invalid TLS config: %s", err)
		}

		schedulerAddr = app.Start(
			app.WithHealthAddr("localhost:0"),
			app.WithCUPSUrl(dataSource.URL),
			app.WithPollingInterval(time.Millisecond),
			app.WithAdapterAddrs([]string{lis.Addr().String()}),
			app.WithTLSConfig(tlsConfig),
		)
	})

	It("reports health info", func() {
		f := func() []byte {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", schedulerAddr))
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := ioutil.ReadAll(resp.Body)
			Expect(err).ToNot(HaveOccurred())
			return body
		}
		Eventually(f).Should(MatchJSON(`{"drainCount": 1, "adapterCount": 1}`))
	})

	It("writes bindings to the adapter", func() {
		expectedRequest := &v1.CreateBindingRequest{
			Binding: &v1.Binding{
				AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
				Drain:    "syslog://some.other.url",
				Hostname: "org.space.logspinner",
			},
		}

		Eventually(testServer.ActualCreateBindingRequest).Should(
			Receive(Equal(expectedRequest)),
		)
	})
})

func TestEndtoend(t *testing.T) {
	log.SetOutput(GinkgoWriter)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scheduler Suite")
}
