package app_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/scalable-syslog/internal/api"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metric"
	"code.cloudfoundry.org/scalable-syslog/scheduler/app"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Scheduler", func() {
	It("reports health info", func() {
		dataSource := httptest.NewServer(&fakeCC{
			results: results{
				"9be15160-4845-4f05-b089-40e827ba61f1": appBindings{
					Hostname: "org.space.name",
					Drains:   []string{"syslog://1.1.1.1/"},
				},
			},
		})
		healthAddr, _ := startScheduler(dataSource.URL, defaultOps())

		f := func() []byte {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", healthAddr))
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := ioutil.ReadAll(resp.Body)
			Expect(err).ToNot(HaveOccurred())
			return body
		}
		Eventually(f, 3*time.Second, 500*time.Millisecond).Should(MatchJSON(`
				{
					"drainCount": 1,
					"adapterCount": 1,
					"blacklistedOrInvalidUrlCount": 0
				}
			`))
	})

	It("ignores blacklisted syslog URLs", func() {
		dataSource := httptest.NewServer(&fakeCC{
			results: results{
				"9be15160-4845-4f05-b089-40e827ba61f1": appBindings{
					Hostname: "org.space.name",
					Drains: []string{
						"syslog://1.1.1.1/",
						"syslog://14.15.16.18/", // blacklisted
					},
				},
			},
		})
		blacklistIPs, err := ingress.NewIPRanges(
			ingress.IPRange{
				Start: "14.15.16.17",
				End:   "14.15.16.20",
			},
		)
		Expect(err).ToNot(HaveOccurred())
		opts := defaultOps()
		opts = append(opts, app.WithBlacklist(blacklistIPs))
		_, testAdapterServer := startScheduler(dataSource.URL, opts)

		expectedRequests := []*v1.CreateBindingRequest{
			{
				Binding: &v1.Binding{
					AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
					Drain:    "syslog://1.1.1.1/",
					Hostname: "org.space.name",
				},
			},
		}
		lenCheck := func() int {
			return len(testAdapterServer.ActualCreateBindingRequest)
		}
		Eventually(lenCheck).Should(Equal(1))
		var actualRequests []*v1.CreateBindingRequest
		f := func() []*v1.CreateBindingRequest {
			select {
			case req := <-testAdapterServer.ActualCreateBindingRequest:
				actualRequests = append(actualRequests, req)
			default:
			}
			return actualRequests
		}
		Eventually(f).Should(ConsistOf(expectedRequests))
	})

	It("services only URLs with drain-version=2.0 when opt-in is enabled", func() {
		dataSource := httptest.NewServer(&fakeCC{
			results: results{
				"9be15160-4845-4f05-b089-40e827ba61f1": appBindings{
					Hostname: "org.space.name",
					Drains: []string{
						"syslog://1.1.1.1/?drain-version=2.0",
						"syslog://2.2.2.2/",
					},
				},
			},
		})
		opts := append(defaultOps(), app.WithOptIn(true))
		_, testAdapterServer := startScheduler(dataSource.URL, opts)
		expectedRequests := []*v1.CreateBindingRequest{
			{
				Binding: &v1.Binding{
					AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
					Drain:    "syslog://1.1.1.1/?drain-version=2.0",
					Hostname: "org.space.name",
				},
			},
		}
		lenCheck := func() int {
			return len(testAdapterServer.ActualCreateBindingRequest)
		}
		Eventually(lenCheck).Should(Equal(1))
		var actualRequests []*v1.CreateBindingRequest
		f := func() []*v1.CreateBindingRequest {
			select {
			case req := <-testAdapterServer.ActualCreateBindingRequest:
				actualRequests = append(actualRequests, req)
			default:
			}
			return actualRequests
		}
		Eventually(f).Should(ConsistOf(expectedRequests))
	})

	It("tells the adapters to delete bindings", func() {
		dataSource := httptest.NewServer(&fakeCC{
			withEmptyResult: true,
			results: results{
				"9be15160-4845-4f05-b089-40e827ba61f1": appBindings{
					Hostname: "org.space.name",
					Drains: []string{
						"syslog://1.1.1.1/",
						"syslog://2.2.2.2/",
					},
				},
			},
		})
		_, testAdapterServer := startScheduler(dataSource.URL, defaultOps())
		expectedRequests := []*v1.DeleteBindingRequest{
			{
				Binding: &v1.Binding{
					AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
					Drain:    "syslog://1.1.1.1/",
					Hostname: "org.space.name",
				},
			},
			{
				Binding: &v1.Binding{
					AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
					Drain:    "syslog://2.2.2.2/",
					Hostname: "org.space.name",
				},
			},
		}
		Eventually(testAdapterServer.ActualDeleteBindingRequest).Should(HaveLen(2))
		var actualRequests []*v1.DeleteBindingRequest
		f := func() []*v1.DeleteBindingRequest {
			select {
			case req := <-testAdapterServer.ActualDeleteBindingRequest:
				actualRequests = append(actualRequests, req)
			default:
			}
			return actualRequests
		}
		Eventually(f).Should(ConsistOf(expectedRequests))
	})

	It("removes old bindings and create new bindings when an app is renamed", func() {
		dataSource := httptest.NewServer(&fakeCC{
			withRenamedApps: true,
		})
		_, testAdapterServer := startScheduler(dataSource.URL, defaultOps())

		createReq := <-testAdapterServer.ActualCreateBindingRequest
		Expect(createReq.Binding).To(Equal(&v1.Binding{
			AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
			Hostname: "org.space.original",
			Drain:    "syslog://14.15.16.22/",
		}))

		deleteReq := <-testAdapterServer.ActualDeleteBindingRequest
		Expect(deleteReq.Binding).To(Equal(&v1.Binding{
			AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
			Hostname: "org.space.original",
			Drain:    "syslog://14.15.16.22/",
		}))

		createReq = <-testAdapterServer.ActualCreateBindingRequest
		Expect(createReq.Binding).To(Equal(&v1.Binding{
			AppId:    "9be15160-4845-4f05-b089-40e827ba61f1",
			Hostname: "org.space.new",
			Drain:    "syslog://14.15.16.22/",
		}))
	})
})

type results map[string]appBindings

type appBindings struct {
	Drains   []string `json:"drains"`
	Hostname string   `json:"hostname"`
}

type fakeCC struct {
	count           int
	called          bool
	withEmptyResult bool
	withRenamedApps bool
	results         results
}

func (f *fakeCC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/internal/v4/syslog_drain_urls" {
		w.WriteHeader(500)
		return
	}

	if f.withRenamedApps {
		f.serveWithRenamedApps(w, r)
		return
	}

	f.serveWithResults(w, r)
}

func (f *fakeCC) serveWithRenamedApps(w http.ResponseWriter, r *http.Request) {
	if !f.called {
		w.Write([]byte(`
			{
				"results": {
					"9be15160-4845-4f05-b089-40e827ba61f1": {
						"drains": [
							"syslog://14.15.16.22/"
						],
						"hostname": "org.space.original"
					}
				}
			}
		`))
		f.called = true
		return
	}

	w.Write([]byte(`
		{
			"results": {
				"9be15160-4845-4f05-b089-40e827ba61f1": {
					"drains": [
						"syslog://14.15.16.22/"
					],
					"hostname": "org.space.new"
				}
			}
		}
	`))
}

func (f *fakeCC) serveWithResults(w http.ResponseWriter, r *http.Request) {
	resultData, err := json.Marshal(struct {
		Results results `json:"results"`
	}{
		Results: f.results,
	})
	if err != nil {
		w.WriteHeader(500)
		return
	}

	if f.count > 0 {
		resultData = []byte(`{"results": {}}`)
	}

	w.Write(resultData)
	if f.withEmptyResult {
		f.count++
	}
}

func defaultOps() []app.SchedulerOption {
	return []app.SchedulerOption{
		app.WithHealthAddr("localhost:0"),
		app.WithPollingInterval(time.Millisecond),
	}
}

func startScheduler(dataSourceURL string, opts []app.SchedulerOption) (string, *testAdapterServer) {
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

	testAdapterServer := NewTestAdapterServer()
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(adapterTLSConfig)),
	)
	v1.RegisterAdapterServer(grpcServer, testAdapterServer)

	go grpcServer.Serve(lis)

	tlsConfig, err := api.NewMutualTLSConfig(
		Cert("scheduler.crt"),
		Cert("scheduler.key"),
		Cert("scalable-syslog-ca.crt"),
		"adapter",
	)
	Expect(err).ToNot(HaveOccurred())

	scheduler := app.NewScheduler(
		dataSourceURL,
		[]string{lis.Addr().String()},
		tlsConfig,
		&metric.Emitter{},
		opts...,
	)
	return scheduler.Start(), testAdapterServer
}
