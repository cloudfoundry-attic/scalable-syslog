package app_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
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
		bindings      []*v1.Binding
	)

	BeforeEach(func() {
		bindings = []*v1.Binding{
			{
				AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
				Drain:    "syslog://new.drain.url/?drain-version=2.0",
				Hostname: "org.space.logspinner",
			},
			{
				AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
				Drain:    "syslog://another.new.drain.url/?drain-version=2.0",
				Hostname: "org.space.logspinner",
			},
		}
	})

	JustBeforeEach(func() {
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
		Expect(err).ToNot(HaveOccurred())

		schedulerAddr = app.Start(
			dataSource.URL,
			[]string{lis.Addr().String()},
			tlsConfig,
			app.WithHealthAddr("localhost:0"),
			app.WithPollingInterval(time.Millisecond),
		)
	})

	Context("when CC continuously returns data", func() {
		BeforeEach(func() {
			dataSource = httptest.NewServer(&fakeCC{})
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
			Eventually(f, 3*time.Second, 500*time.Millisecond).Should(MatchJSON(`{"drainCount": 2, "adapterCount": 1}`))
		})

		It("writes drain-version=2.0 bindings to the adapter", func() {
			expectedRequests := []*v1.CreateBindingRequest{
				{
					Binding: bindings[0],
				},
				{
					Binding: bindings[1],
				},
			}
			// TODO: when we implement diffing in the scheduler this will need
			// to change to HaveLen(2)
			lenCheck := func() int {
				return len(testServer.ActualCreateBindingRequest)
			}
			Eventually(lenCheck).Should(BeNumerically(">=", 2))
			var actualRequests []*v1.CreateBindingRequest
			f := func() []*v1.CreateBindingRequest {
				select {
				case req := <-testServer.ActualCreateBindingRequest:
					actualRequests = append(actualRequests, req)
				default:
				}
				return actualRequests
			}
			Eventually(f).Should(ConsistOf(expectedRequests))
		})
	})

	Context("with a CC that starts returning an empty result", func() {
		BeforeEach(func() {
			dataSource = httptest.NewServer(&fakeCC{
				withEmptyResult: true,
			})
		})

		It("tells the adapters to delete the removed binding", func() {
			expectedRequests := []*v1.DeleteBindingRequest{
				{
					Binding: bindings[0],
				},
				{
					Binding: bindings[1],
				},
			}
			Eventually(testServer.ActualDeleteBindingRequest).Should(HaveLen(2))
			var actualRequests []*v1.DeleteBindingRequest
			f := func() []*v1.DeleteBindingRequest {
				select {
				case req := <-testServer.ActualDeleteBindingRequest:
					actualRequests = append(actualRequests, req)
				default:
				}
				return actualRequests
			}
			Eventually(f).Should(ConsistOf(expectedRequests))
		})
	})
})

type fakeCC struct {
	count           int
	withEmptyResult bool
}

func (f *fakeCC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.count > 0 {
		w.Write([]byte(`
			{
			  "results": {}
			}
		`))
		return
	}
	w.Write([]byte(`
		{
		  "results": {
			"9be15160-4845-4f05-b089-40e827ba61f1": {
			  "drains": [
				"syslog://new.drain.url/?drain-version=2.0",
				"syslog://another.new.drain.url/?drain-version=2.0",
				"syslog://legacy.drain.url"
			  ],
			  "hostname": "org.space.logspinner"
			},
			"ed150c22-f866-11e6-bc64-92361f002671": {
			  "drains": [
				"syslog://legacy.drain.url"
			  ],
			  "hostname": "org.space.logspinner"
			}
		  }
		}
	`))
	if f.withEmptyResult {
		f.count++
	}
}
