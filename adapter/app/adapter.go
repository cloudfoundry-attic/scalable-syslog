package app

import (
	"crypto/tls"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/binding"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/ingress"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/timeoutwaitgroup"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/health"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type MetricClient interface {
	NewCounterMetric(string, ...pulseemitter.MetricOption) *pulseemitter.CounterMetric
}

type Adapter struct {
	mu                sync.Mutex
	healthAddr        string
	adapterServerAddr string

	ctx    context.Context
	cancel func()

	adapterServer          *grpc.Server
	bindingManager         *binding.BindingManager
	logsAPIConnCount       int
	logsAPIConnTTL         time.Duration
	logsEgressAPIAddr      string
	logsEgressAPITLSConfig *tls.Config
	adapterServerTLSConfig *tls.Config
	syslogDialTimeout      time.Duration
	syslogIOTimeout        time.Duration
	skipCertVerify         bool
	health                 *health.Health
	metricClient           MetricClient
	timeoutWaitGroup       *timeoutwaitgroup.TimeoutWaitGroup
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
	metricClient MetricClient,
	opts ...AdapterOption,
) *Adapter {
	ctx, cancel := context.WithCancel(context.Background())

	a := &Adapter{
		healthAddr:             ":8080",
		adapterServerAddr:      ":443",
		ctx:                    ctx,
		cancel:                 cancel,
		logsAPIConnCount:       10,
		logsAPIConnTTL:         600 * time.Second,
		logsEgressAPIAddr:      logsEgressAPIAddr,
		logsEgressAPITLSConfig: logsEgressAPITLSConfig,
		adapterServerTLSConfig: adapterServerTLSConfig,
		syslogDialTimeout:      5 * time.Second,
		syslogIOTimeout:        60 * time.Second,
		skipCertVerify:         true,
		health:                 health.NewHealth(),
		timeoutWaitGroup:       timeoutwaitgroup.New(time.Minute),
	}

	for _, o := range opts {
		o(a)
	}

	balancer := ingress.NewBalancer(a.logsEgressAPIAddr)
	connector := ingress.NewConnector(balancer, a.logsEgressAPITLSConfig)
	clientManager := ingress.NewClientManager(
		connector,
		a.logsAPIConnCount,
		a.logsAPIConnTTL,
		time.Second)

	constructors := map[string]egress.SyslogConstructor{
		"https":      egress.NewHTTPSWriter,
		"syslog":     egress.NewTCPWriter,
		"syslog-tls": egress.NewTLSWriter,
	}

	droppedMetrics := map[string]*pulseemitter.CounterMetric{
		// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
		// when sending to a syslog drain over https.
		"https": buildMetric(metricClient, "https", "dropped"),
		// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
		// when sending to a syslog drain over syslog.
		"syslog": buildMetric(metricClient, "syslog", "dropped"),
		// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
		// when sending to a syslog drain over syslog-tls.
		"syslog-tls": buildMetric(metricClient, "syslog-tls", "dropped"),
	}

	egressMetrics := map[string]*pulseemitter.CounterMetric{
		// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
		// to a syslog drain over https.
		"https": buildMetric(metricClient, "https", "egress"),
		// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
		// to a syslog drain over syslog.
		"syslog": buildMetric(metricClient, "syslog", "egress"),
		// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
		// to a syslog drain over syslog-tls.
		"syslog-tls": buildMetric(metricClient, "syslog-tls", "egress"),
	}

	syslogConnector := egress.NewSyslogConnector(
		a.syslogDialTimeout,
		a.syslogIOTimeout,
		a.skipCertVerify,
		a.timeoutWaitGroup,
		egress.WithConstructors(constructors),
		egress.WithDroppedMetrics(droppedMetrics),
		egress.WithEgressMetrics(egressMetrics),
	)
	subscriber := ingress.NewSubscriber(a.ctx, clientManager, syslogConnector, metricClient)

	a.bindingManager = binding.NewBindingManager(subscriber)
	a.healthAddr = health.StartServer(a.health, a.healthAddr)

	return a
}

func buildMetric(m MetricClient, protocol, name string) *pulseemitter.CounterMetric {
	return m.NewCounterMetric(name,
		pulseemitter.WithVersion(2, 0),
		pulseemitter.WithTags(map[string]string{"drain-protocol": protocol}),
	)
}

// Start starts the adapter health endpoint and gRPC service.
func (a *Adapter) Start() error {
	lis, err := net.Listen("tcp", a.adapterServerAddr) // close this listener
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	adapterServer := binding.NewAdapterServer(a.bindingManager, a.health)
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(a.adapterServerTLSConfig)),
	)
	v1.RegisterAdapterServer(grpcServer, adapterServer)

	log.Printf("Adapter server is listening on %s", lis.Addr().String())
	a.adapterServer = grpcServer

	a.mu.Lock()
	a.adapterServerAddr = lis.Addr().String()
	a.mu.Unlock()

	return grpcServer.Serve(lis)
}

func (a *Adapter) HealthAddr() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.healthAddr
}

func (a *Adapter) ServerAddr() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.adapterServerAddr
}

func (a *Adapter) Stop() {
	log.Printf("Draining connections...")

	a.adapterServer.Stop()
	a.cancel()
	a.timeoutWaitGroup.Wait()

	log.Printf("Done draining connections.")
	log.Println("Shutting down adapter server")
}
