package app

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/bindingmanager"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/controller"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/health"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/ingress"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Adapter struct {
	healthAddr             string
	logsAPIConnCount       int
	logsAPIConnTTL         time.Duration
	controllerAddr         string
	logsEgressAPIAddr      string
	logsEgressAPITLSConfig *tls.Config
	controllerTLSConfig    *tls.Config
	syslogDialTimeout      time.Duration
	syslogIOTimeout        time.Duration
	skipCertVerify         bool
}

// AdapterOption is a type that will manipulate a config
type AdapterOption func(*Adapter)

// WithHealthAddr sets the address for the health endpoint to bind to.
func WithHealthAddr(addr string) func(*Adapter) {
	return func(c *Adapter) {
		c.healthAddr = addr
	}
}

// WithServiceAddr sets the address for the gRPC controller to bind to.
func WithControllerAddr(addr string) func(*Adapter) {
	return func(c *Adapter) {
		c.controllerAddr = addr
	}
}

// WithLogsEgressAPIConnCount sets the maximum number of connections to the
// Loggregator API
func WithLogsEgressAPIConnCount(m int) func(*Adapter) {
	return func(c *Adapter) {
		c.logsAPIConnCount = m
	}
}

// WithLogsEgressAPIConnTTL sets the number of seconds for a connection to the
// Loggregator API to live
func WithLogsEgressAPIConnTTL(d int) func(*Adapter) {
	return func(c *Adapter) {
		c.logsAPIConnTTL = time.Duration(int64(d)) * time.Second
	}
}

// WithSyslogDialTimeout sets the TCP dial timeout for connecting to a syslog
// drain
func WithSyslogDialTimeout(d time.Duration) func(*Adapter) {
	return func(a *Adapter) {
		a.syslogDialTimeout = d
	}
}

// WithSyslogIOTimeout sets the TCP IO timeout for writing to a syslog
// drain
func WithSyslogIOTimeout(d time.Duration) func(*Adapter) {
	return func(a *Adapter) {
		a.syslogIOTimeout = d
	}
}

// WithSyslogSkipCertVerify sets the TCP InsecureSkipVerify property for
// syslog
func WithSyslogSkipCertVerify(b bool) func(*Adapter) {
	return func(a *Adapter) {
		a.skipCertVerify = b
	}
}

// NewAdapter returns an Adapter
func NewAdapter(
	logsEgressAPIAddr string,
	logsEgressAPITLSConfig *tls.Config,
	controllerTLSConfig *tls.Config,
	opts ...AdapterOption,
) *Adapter {
	adapter := &Adapter{
		healthAddr:             ":8080",
		controllerAddr:         ":443",
		logsAPIConnCount:       5,
		logsAPIConnTTL:         600 * time.Second,
		logsEgressAPIAddr:      logsEgressAPIAddr,
		logsEgressAPITLSConfig: logsEgressAPITLSConfig,
		controllerTLSConfig:    controllerTLSConfig,
		syslogDialTimeout:      1 * time.Second,
		syslogIOTimeout:        60 * time.Second,
		skipCertVerify:         true,
	}

	for _, o := range opts {
		o(adapter)
	}

	return adapter
}

// Start starts the adapter health endpoint and gRPC service.
func (a *Adapter) Start() (actualHealth, actualService string) {
	log.Print("Starting adapter...")

	balancer := ingress.NewBalancer(a.logsEgressAPIAddr)
	connector := ingress.NewConnector(
		balancer,
		grpc.WithTransportCredentials(credentials.NewTLS(a.logsEgressAPITLSConfig)),
	)
	clientManager := ingress.NewClientManager(connector, a.logsAPIConnCount, a.logsAPIConnTTL)
	dialer := &net.Dialer{Timeout: a.syslogDialTimeout}
	builder := egress.NewWriterBuilder(
		a.syslogIOTimeout,
		a.skipCertVerify,
		egress.WithTCPOptions(
			egress.WithDialFunc(func(addr string) (net.Conn, error) {
				return dialer.Dial("tcp", addr)
			}),
		),
	)
	subscriber := ingress.NewSubscriber(clientManager, builder)
	manager := bindingmanager.New(subscriber)

	actualHealth = startHealthServer(a.healthAddr, manager)
	creds := credentials.NewTLS(a.controllerTLSConfig)
	actualService = startAdapterService(a.controllerAddr, creds, manager)

	return actualHealth, actualService
}

func startHealthServer(hostport string, manager *bindingmanager.BindingManager) string {
	l, err := net.Listen("tcp", hostport)
	if err != nil {
		log.Fatalf("Unable to setup Health endpoint (%s): %s", hostport, err)
	}

	server := http.Server{
		Addr:         hostport,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	router := http.NewServeMux()
	router.Handle("/health", health.NewHealth(manager))
	server.Handler = router

	go func() {
		log.Fatalf("Health server closing: %s", server.Serve(l))
	}()

	log.Printf("Health endpoint is listening on %s", l.Addr().String())
	return l.Addr().String()
}

func startAdapterService(hostport string, creds credentials.TransportCredentials, manager *bindingmanager.BindingManager) string {
	lis, err := net.Listen("tcp", hostport)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	adapterService := controller.New(manager)
	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
	)
	v1.RegisterAdapterServer(grpcServer, adapterService)

	go func() {
		log.Fatalf("failed to serve: %v", grpcServer.Serve(lis))
	}()

	log.Printf("Adapter controller is listening on %s", lis.Addr().String())
	return lis.Addr().String()
}
