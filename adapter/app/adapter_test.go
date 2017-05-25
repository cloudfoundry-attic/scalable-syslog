package app_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/scalable-syslog/adapter/app"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/test_util"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
	v2 "code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/crewjam/rfc5424"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Adapter", func() {
	var (
		logsAPIAddr  string
		rlpTLSConfig *tls.Config
		tlsConfig    *tls.Config

		adapterHealthAddr string
		client            v1.AdapterClient
		binding           *v1.Binding
		egressServer      *MockEgressServer
		syslogTCPServer   *SyslogTCPServer
	)

	BeforeEach(func() {
		egressServer, logsAPIAddr = startLogsAPIServer()

		var err error
		rlpTLSConfig, err = api.NewMutualTLSConfig(
			test_util.Cert("adapter-rlp.crt"),
			test_util.Cert("adapter-rlp.key"),
			test_util.Cert("loggregator-ca.crt"),
			"fake-log-provider",
		)
		Expect(err).ToNot(HaveOccurred())

		tlsConfig, err = api.NewMutualTLSConfig(
			test_util.Cert("adapter.crt"),
			test_util.Cert("adapter.key"),
			test_util.Cert("scalable-syslog-ca.crt"),
			"fake-log-provider",
		)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("health endpoint", func() {
		BeforeEach(func() {
			adapter := app.NewAdapter(
				logsAPIAddr,
				rlpTLSConfig,
				tlsConfig,
				testhelper.NewMetricClient(),
				app.WithHealthAddr("localhost:0"),
				app.WithAdapterServerAddr("localhost:0"),
				app.WithLogsEgressAPIConnCount(1),
			)
			go adapter.Start()
			Eventually(adapter.ServerAddr).ShouldNot(Equal("localhost:0"))

			adapterHealthAddr = adapter.HealthAddr()
			client = startAdapterClient(adapter.ServerAddr())
			syslogTCPServer = newSyslogTCPServer()

			binding = &v1.Binding{
				AppId:    "app-guid",
				Hostname: "a-hostname",
				Drain:    "syslog://" + syslogTCPServer.addr().String(),
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
	})

	Context("with TLS drain", func() {
		BeforeEach(func() {
			syslogTCPServer = newSyslogTLSServer()

			binding = &v1.Binding{
				AppId:    "appguid",
				Hostname: "ahostname",
				Drain:    "syslog-tls://" + syslogTCPServer.addr().String(),
			}
		})

		Context("with skip ssl validation enabled", func() {
			BeforeEach(func() {
				adapter := app.NewAdapter(
					logsAPIAddr,
					rlpTLSConfig,
					tlsConfig,
					testhelper.NewMetricClient(),
					app.WithHealthAddr("localhost:0"),
					app.WithAdapterServerAddr("localhost:0"),
					app.WithLogsEgressAPIConnCount(1),
					app.WithSyslogSkipCertVerify(true),
				)
				go adapter.Start()

				Eventually(adapter.ServerAddr).ShouldNot(Equal("localhost:0"))
				client = startAdapterClient(adapter.ServerAddr())
			})

			It("forwards logs from loggregator to a syslog TLS drain", func() {
				By("creating a binding", func() {
					_, err := client.CreateBinding(context.Background(), &v1.CreateBindingRequest{
						Binding: binding,
					})
					Expect(err).ToNot(HaveOccurred())

					Eventually(syslogTCPServer.msgCount).Should(BeNumerically(">", 10))
				})

				By("deleting a binding", func() {
					_, err := client.DeleteBinding(context.Background(), &v1.DeleteBindingRequest{
						Binding: binding,
					})
					Expect(err).ToNot(HaveOccurred())

					previousVal := uint64(0)
					Eventually(func() bool {
						defer func() { previousVal = syslogTCPServer.msgCount() }()
						return previousVal == syslogTCPServer.msgCount()
					}).Should(BeTrue())

					currentCount := syslogTCPServer.msgCount()
					Consistently(syslogTCPServer.msgCount, "100ms").Should(BeNumerically("~", currentCount, 2))
				})
			})
		})

		Context("with skip ssl validation disabled", func() {
			BeforeEach(func() {
				adapter := app.NewAdapter(
					logsAPIAddr,
					rlpTLSConfig,
					tlsConfig,
					testhelper.NewMetricClient(),
					app.WithHealthAddr("localhost:0"),
					app.WithAdapterServerAddr("localhost:0"),
					app.WithLogsEgressAPIConnCount(1),
					app.WithSyslogSkipCertVerify(false),
				)
				go adapter.Start()

				Eventually(adapter.ServerAddr).ShouldNot(Equal("localhost:0"))
				client = startAdapterClient(adapter.ServerAddr())
			})

			It("fails to forward logs", func() {
				_, err := client.CreateBinding(context.Background(), &v1.CreateBindingRequest{
					Binding: binding,
				})
				Expect(err).ToNot(HaveOccurred())

				Consistently(syslogTCPServer.msgCount).Should(Equal(uint64(0)))
			})
		})

		Describe("drain and die", func() {
			var adapter *app.Adapter

			BeforeEach(func() {
				syslogTCPServer = newSyslogTLSServer()

				binding = &v1.Binding{
					AppId:    "appguid",
					Hostname: "ahostname",
					Drain:    "syslog-tls://" + syslogTCPServer.addr().String(),
				}

				adapter = app.NewAdapter(
					logsAPIAddr,
					rlpTLSConfig,
					tlsConfig,
					testhelper.NewMetricClient(),
					app.WithHealthAddr("localhost:0"),
					app.WithAdapterServerAddr("localhost:0"),
					app.WithLogsEgressAPIConnCount(1),
					app.WithSyslogSkipCertVerify(true),
				)

				go adapter.Start()
				Eventually(adapter.ServerAddr).ShouldNot(Equal("localhost:0"))

				client = startAdapterClient(adapter.ServerAddr())
				_, err := client.CreateBinding(context.Background(), &v1.CreateBindingRequest{
					Binding: binding,
				})
				Expect(err).ToNot(HaveOccurred())
				Eventually(syslogTCPServer.msgCount).Should(BeNumerically(">", 10))
			})

			It("empties buffers and sends logs to syslog", func() {
				adapter.Stop()
				lastIdx := egressServer.WaitForLastSentIdx()
				Eventually(syslogTCPServer.LastReceivedIdx).Should(BeNumerically("~", lastIdx, 1))
			}, 5)
		})
	})
})

func startAdapterClient(addr string) v1.AdapterClient {
	tlsConfig, err := api.NewMutualTLSConfig(
		test_util.Cert("adapter.crt"),
		test_util.Cert("adapter.key"),
		test_util.Cert("scalable-syslog-ca.crt"),
		"adapter",
	)
	Expect(err).ToNot(HaveOccurred())

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	Expect(err).ToNot(HaveOccurred())

	return v1.NewAdapterClient(conn)
}

func startLogsAPIServer() (*MockEgressServer, string) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	tlsConfig, err := api.NewMutualTLSConfig(
		test_util.Cert("fake-log-provider.crt"),
		test_util.Cert("fake-log-provider.key"),
		test_util.Cert("loggregator-ca.crt"),
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
	receiver       chan v2.Egress_ReceiverServer
	lastSuccessIdx int64
	exited         int64
}

func NewMockEgressServer() *MockEgressServer {
	return &MockEgressServer{
		receiver: make(chan v2.Egress_ReceiverServer, 5),
	}
}

func (m *MockEgressServer) Receiver(req *v2.EgressRequest, receiver v2.Egress_ReceiverServer) error {
	defer atomic.StoreInt64(&m.exited, 1)
	m.receiver <- receiver

	for i := 0; ; i++ {
		err := receiver.Send(buildLogEnvelope(int64(i)))
		if err != nil {
			return err
		}
		atomic.StoreInt64(&m.lastSuccessIdx, int64(i))
		time.Sleep(time.Millisecond)
	}
}

func (m *MockEgressServer) WaitForLastSentIdx() int64 {
	for atomic.LoadInt64(&m.exited) < 1 {
		time.Sleep(10 * time.Millisecond)
	}

	return atomic.LoadInt64(&m.lastSuccessIdx)
}

type SyslogTCPServer struct {
	lis             net.Listener
	mu              sync.Mutex
	msgCount_       uint64
	lastReceivedIdx int64
}

func newSyslogTCPServer() *SyslogTCPServer {
	lis, err := net.Listen("tcp", ":0")
	Expect(err).ToNot(HaveOccurred())
	m := &SyslogTCPServer{
		lis: lis,
	}
	go m.accept()
	return m
}

func newSyslogTLSServer() *SyslogTCPServer {
	lis, err := net.Listen("tcp", ":0")
	Expect(err).ToNot(HaveOccurred())
	cert, err := tls.LoadX509KeyPair(
		test_util.Cert("adapter.crt"),
		test_util.Cert("adapter.key"),
	)
	Expect(err).ToNot(HaveOccurred())
	tlsLis := tls.NewListener(lis, &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	m := &SyslogTCPServer{
		lis: tlsLis,
	}
	go m.accept()
	return m
}

func (m *SyslogTCPServer) LastReceivedIdx() int64 {
	return atomic.LoadInt64(&m.lastReceivedIdx)
}

func (m *SyslogTCPServer) accept() {
	for {
		conn, err := m.lis.Accept()
		if err != nil {
			return
		}
		go m.handleConn(conn)
	}
}

func (m *SyslogTCPServer) handleConn(conn net.Conn) {
	for {
		var msg rfc5424.Message

		_, err := msg.ReadFrom(conn)
		if err != nil {
			return
		}

		i, err := strconv.ParseInt(strings.TrimSpace(string(msg.Message)), 10, 64)
		if err == nil {
			atomic.StoreInt64(&m.lastReceivedIdx, i)
		}

		if err != nil {
			fmt.Println("Failed to parse", err)
		}

		atomic.AddUint64(&m.msgCount_, 1)
	}
}

func (m *SyslogTCPServer) msgCount() uint64 {
	return atomic.LoadUint64(&m.msgCount_)
}

func (m *SyslogTCPServer) addr() net.Addr {
	return m.lis.Addr()
}

func buildLogEnvelope(i int64) *v2.Envelope {
	return &v2.Envelope{
		Tags: map[string]*v2.Value{
			"source_type":     {&v2.Value_Text{"APP"}},
			"source_instance": {&v2.Value_Text{"2"}},
		},
		Timestamp: 12345678,
		SourceId:  "app-guid",
		Message: &v2.Envelope_Log{
			Log: &v2.Log{
				Payload: []byte(fmt.Sprint(i)),
				Type:    v2.Log_OUT,
			},
		},
	}
}
