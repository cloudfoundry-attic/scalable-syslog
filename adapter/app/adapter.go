package app

import (
	"crypto/tls"
	"log"
	"net"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/binding"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/ingress"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/health"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/metric"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Adapter struct {
	healthAddr             string
	logsAPIConnCount       int
	logsAPIConnTTL         time.Duration
	adapterServerAddr      string
	logsEgressAPIAddr      string
	logsEgressAPITLSConfig *tls.Config
	adapterServerTLSConfig *tls.Config
	syslogDialTimeout      time.Duration
	syslogIOTimeout        time.Duration
	skipCertVerify         bool
	health                 *health.Health
	emitter                *metric.Emitter
}

// AdapterOption is a type that will manipulate a config
type AdapterOption func(*Adapter)

// WithHealthAddr sets the address for the health endpoint to bind to.
func WithHealthAddr(addr string) func(*Adapter) {
	return func(c *Adapter) {
		c.healthAddr = addr
	}
}

// WithAdapterServerAddr sets the address for the gRPC server to bind to.
func WithAdapterServerAddr(addr string) func(*Adapter) {
	return func(c *Adapter) {
		c.adapterServerAddr = addr
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
	adapterServerTLSConfig *tls.Config,
	emitter *metric.Emitter,
	opts ...AdapterOption,
) *Adapter {
	adapter := &Adapter{
		healthAddr:             ":8080",
		adapterServerAddr:      ":443",
		logsAPIConnCount:       5,
		logsAPIConnTTL:         600 * time.Second,
		logsEgressAPIAddr:      logsEgressAPIAddr,
		logsEgressAPITLSConfig: logsEgressAPITLSConfig,
		adapterServerTLSConfig: adapterServerTLSConfig,
		syslogDialTimeout:      5 * time.Second,
		syslogIOTimeout:        60 * time.Second,
		skipCertVerify:         true,
		health:                 health.NewHealth(),
		emitter:                emitter,
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
	clientManager := ingress.NewClientManager(
		connector,
		a.logsAPIConnCount,
		a.logsAPIConnTTL,
	)
	syslogConnector := egress.NewSyslogConnector(
		a.syslogDialTimeout,
		a.syslogIOTimeout,
		a.skipCertVerify,
		a.emitter,
	)
	subscriber := ingress.NewSubscriber(clientManager, syslogConnector, a.emitter)
	manager := binding.NewBindingManager(subscriber)

	actualHealth = health.StartServer(a.health, a.healthAddr)
	creds := credentials.NewTLS(a.adapterServerTLSConfig)
	actualService = a.startAdapterService(creds, manager)

	return actualHealth, actualService
}

func (a *Adapter) startAdapterService(creds credentials.TransportCredentials, manager *binding.BindingManager) string {
	lis, err := net.Listen("tcp", a.adapterServerAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	adapterServer := binding.NewAdapterServer(manager, a.health)
	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
	)
	v1.RegisterAdapterServer(grpcServer, adapterServer)

	go func() {
		log.Fatalf("failed to serve: %v", grpcServer.Serve(lis))
	}()

	log.Printf("Adapter server is listening on %s", lis.Addr().String())
	return lis.Addr().String()
}
