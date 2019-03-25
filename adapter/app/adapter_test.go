package app_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	loggregator "code.cloudfoundry.org/go-loggregator"
	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/rfc5424"
	"code.cloudfoundry.org/scalable-syslog/adapter/app"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/test_util"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Adapter", func() {
	var (
		logsAPIAddr      string
		rlpTLSConfig     *tls.Config
		adapterTlsConfig *tls.Config

		adapterHealthAddr string
		client            v1.AdapterClient
		binding           *v1.Binding
		syslogTCPServer   *SyslogTCPServer

		lastIdx *int64
	)

	BeforeEach(func() {
		_, logsAPIAddr, lastIdx = startLogsAPIServer()

		var err error
		rlpTLSConfig, err = api.NewMutualTLSConfig(
			test_util.Cert("adapter-rlp.crt"),
			test_util.Cert("adapter-rlp.key"),
			test_util.Cert("loggregator-ca.crt"),
			"fake-log-provider",
		)
		Expect(err).ToNot(HaveOccurred())

		adapterTlsConfig, err = api.NewMutualTLSConfig(
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
				logsAPIAddr,
				rlpTLSConfig,
				adapterTlsConfig,
				testhelper.NewMetricClient(),
				&spyLogClient{},
				"instance",
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
				AppId:    "app-guid",
				Hostname: "ahostname",
				Drain:    "syslog-tls://" + syslogTCPServer.addr().String(),
			}
		})

		Context("with skip ssl validation enabled", func() {
			BeforeEach(func() {
				adapter := app.NewAdapter(
					logsAPIAddr,
					logsAPIAddr,
					rlpTLSConfig,
					adapterTlsConfig,
					testhelper.NewMetricClient(),
					&spyLogClient{},
					"instance",
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
					}, 1, "50ms").Should(BeTrue())

					currentCount := syslogTCPServer.msgCount()
					Consistently(syslogTCPServer.msgCount, "100ms").Should(BeNumerically("~", currentCount, 2))
				})
			})
		})

		Context("with skip ssl validation disabled", func() {
			BeforeEach(func() {
				adapter := app.NewAdapter(
					logsAPIAddr,
					logsAPIAddr,
					rlpTLSConfig,
					adapterTlsConfig,
					testhelper.NewMetricClient(),
					&spyLogClient{},
					"instance",
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
					AppId:    "app-guid",
					Hostname: "ahostname",
					Drain:    "syslog-tls://" + syslogTCPServer.addr().String(),
				}

				adapter = app.NewAdapter(
					logsAPIAddr,
					logsAPIAddr,
					rlpTLSConfig,
					adapterTlsConfig,
					testhelper.NewMetricClient(),
					&spyLogClient{},
					"instance",
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

			It("empties buffers and sends logs to syslog", func(done Done) {
				defer close(done)

				adapter.Stop()
				idx := waitForLastSentIdx(lastIdx)
				Eventually(syslogTCPServer.LastReceivedIdx).Should(BeNumerically("~", idx, 10))
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

func startLogsAPIServer() (*testEgressServer, string, *int64) {
	server, err := newTestEgressServer(
		test_util.Cert("fake-log-provider.crt"),
		test_util.Cert("fake-log-provider.key"),
		test_util.Cert("loggregator-ca.crt"),
		0,
	)
	Expect(err).ToNot(HaveOccurred())

	Expect(server.start()).To(Succeed())

	return server, server.addr(), &server.lastIdx
}

func waitForLastSentIdx(lastIdx *int64) int64 {
	for atomic.LoadInt64(lastIdx) < 1 {
		time.Sleep(10 * time.Millisecond)
	}

	return atomic.LoadInt64(lastIdx)
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
		Tags: map[string]string{
			"source_type":     "APP",
			"source_instance": "2",
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

type spyLogClient struct{}

func (*spyLogClient) EmitLog(string, ...loggregator.EmitLogOption) {
}

type testEgressServer struct {
	addr_      string
	cn         string
	delay      time.Duration
	lastIdx    int64
	tlsConfig  *tls.Config
	grpcServer *grpc.Server
	grpc.Stream
}

type egressServerOption func(*testEgressServer)

func withCN(cn string) egressServerOption {
	return func(s *testEgressServer) {
		s.cn = cn
	}
}

func withAddr(addr string) egressServerOption {
	return func(s *testEgressServer) {
		s.addr_ = addr
	}
}

func newTestEgressServer(serverCert, serverKey, caCert string, delay time.Duration, opts ...egressServerOption) (*testEgressServer, error) {
	s := &testEgressServer{
		addr_: "localhost:0",
		delay: delay,
	}

	for _, o := range opts {
		o(s)
	}

	cert, err := tls.LoadX509KeyPair(serverCert, serverKey)
	if err != nil {
		return nil, err
	}

	s.tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ClientAuth:         tls.RequestClientCert,
		InsecureSkipVerify: false,
		ServerName:         s.cn,
	}
	caCertBytes, err := ioutil.ReadFile(caCert)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertBytes)
	s.tlsConfig.RootCAs = caCertPool

	return s, nil
}

func (t *testEgressServer) addr() string {
	return t.addr_
}

func (t *testEgressServer) Receiver(r *v2.EgressRequest, server v2.Egress_ReceiverServer) error {
	panic("not implemented")

	return nil
}

func (t *testEgressServer) BatchedReceiver(r *v2.EgressBatchRequest, server v2.Egress_BatchedReceiverServer) error {
	for i := 0; ; i++ {
		err := server.Send(
			&v2.EnvelopeBatch{
				Batch: []*v2.Envelope{
					buildLogEnvelope(int64(i)),
				},
			},
		)
		if err != nil {
			// Subtract 1 due to the last one failing
			atomic.StoreInt64(&t.lastIdx, int64(i-1))
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func (t *testEgressServer) start() error {
	listener, err := net.Listen("tcp4", t.addr_)
	if err != nil {
		return err
	}
	t.addr_ = listener.Addr().String()

	var opts []grpc.ServerOption
	if t.tlsConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(t.tlsConfig)))
	}
	t.grpcServer = grpc.NewServer(opts...)

	v2.RegisterEgressServer(t.grpcServer, t)

	go t.grpcServer.Serve(listener)

	return nil
}

func (t *testEgressServer) stop() {
	t.grpcServer.Stop()
}
