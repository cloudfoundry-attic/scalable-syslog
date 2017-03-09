package app_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
	"github.com/cloudfoundry-incubator/scalable-syslog/api"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Adapter", func() {
	var (
		logsAPIAddr  string
		rlpTLSConfig *tls.Config
		tlsConfig    *tls.Config

		adapterServiceHost string
		adapterHealthAddr  string
		client             v1.AdapterClient
		binding            *v1.Binding
		egressServer       *MockEgressServer
		syslogTCPServer    *SyslogTCPServer
	)

	BeforeEach(func() {
		egressServer, logsAPIAddr = startLogsAPIServer()

		var err error
		rlpTLSConfig, err = api.NewMutualTLSConfig(
			Cert("adapter-rlp.crt"),
			Cert("adapter-rlp.key"),
			Cert("loggregator-ca.crt"),
			"fake-log-provider",
		)
		Expect(err).ToNot(HaveOccurred())

		tlsConfig, err = api.NewMutualTLSConfig(
			Cert("adapter.crt"),
			Cert("adapter.key"),
			Cert("scalable-syslog-ca.crt"),
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
				app.WithHealthAddr("localhost:0"),
				app.WithControllerAddr("localhost:0"),
				app.WithLogsEgressAPIConnCount(1),
			)
			adapterHealthAddr, adapterServiceHost = adapter.Start()

			client = startAdapterClient(adapterServiceHost)
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

	Context("with TCP drain", func() {
		BeforeEach(func() {
			adapter := app.NewAdapter(
				logsAPIAddr,
				rlpTLSConfig,
				tlsConfig,
				app.WithHealthAddr("localhost:0"),
				app.WithControllerAddr("localhost:0"),
				app.WithLogsEgressAPIConnCount(1),
			)
			adapterHealthAddr, adapterServiceHost = adapter.Start()

			client = startAdapterClient(adapterServiceHost)
			syslogTCPServer = newSyslogTCPServer()

			binding = &v1.Binding{
				AppId:    "app-guid",
				Hostname: "a-hostname",
				Drain:    "syslog://" + syslogTCPServer.addr().String(),
			}
		})

		It("forwards logs from loggregator to a syslog TCP drain", func() {
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

				currentCount := syslogTCPServer.msgCount()
				Consistently(syslogTCPServer.msgCount, "100ms").Should(BeNumerically("~", currentCount, 2))
			})
		})
	})

	Context("with TLS drain", func() {
		BeforeEach(func() {
			syslogTCPServer = newSyslogTLSServer()

			binding = &v1.Binding{
				AppId:    "app-guid",
				Hostname: "a-hostname",
				Drain:    "syslog-tls://" + syslogTCPServer.addr().String(),
			}
		})

		Context("with skip ssl validation enabled", func() {
			BeforeEach(func() {
				adapter := app.NewAdapter(
					logsAPIAddr,
					rlpTLSConfig,
					tlsConfig,
					app.WithHealthAddr("localhost:0"),
					app.WithControllerAddr("localhost:0"),
					app.WithLogsEgressAPIConnCount(1),
					app.WithSyslogSkipCertVerify(true),
				)
				adapterHealthAddr, adapterServiceHost = adapter.Start()

				client = startAdapterClient(adapterServiceHost)
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
					app.WithHealthAddr("localhost:0"),
					app.WithControllerAddr("localhost:0"),
					app.WithLogsEgressAPIConnCount(1),
					app.WithSyslogSkipCertVerify(false),
				)
				adapterHealthAddr, adapterServiceHost = adapter.Start()

				client = startAdapterClient(adapterServiceHost)
			})

			It("fails to forward logs", func() {
				_, err := client.CreateBinding(context.Background(), &v1.CreateBindingRequest{
					Binding: binding,
				})
				Expect(err).ToNot(HaveOccurred())

				Consistently(syslogTCPServer.msgCount).Should(Equal(uint64(0)))
			})
		})
	})
})

func startAdapterClient(addr string) v1.AdapterClient {
	tlsConfig, err := api.NewMutualTLSConfig(
		Cert("adapter.crt"),
		Cert("adapter.key"),
		Cert("scalable-syslog-ca.crt"),
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

	for {
		Expect(receiver.Send(buildLogEnvelope())).To(Succeed())
		time.Sleep(time.Millisecond)
	}

	return nil
}

type SyslogTCPServer struct {
	lis       net.Listener
	mu        sync.Mutex
	msgCount_ uint64
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
		Cert("adapter.crt"),
		Cert("adapter.key"),
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
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			return
		}
		_ = buf
		atomic.AddUint64(&m.msgCount_, 1)
	}
}

func (m *SyslogTCPServer) msgCount() uint64 {
	return atomic.LoadUint64(&m.msgCount_)
}

func (m *SyslogTCPServer) addr() net.Addr {
	return m.lis.Addr()
}

func buildLogEnvelope() *v2.Envelope {
	return &v2.Envelope{
		Tags: map[string]*v2.Value{
			"source_type":     {&v2.Value_Text{"APP"}},
			"source_instance": {&v2.Value_Text{"2"}},
		},
		Timestamp: 12345678,
		SourceId:  "app-guid",
		Message: &v2.Envelope_Log{
			Log: &v2.Log{
				Payload: []byte("log"),
				Type:    v2.Log_OUT,
			},
		},
	}
}
