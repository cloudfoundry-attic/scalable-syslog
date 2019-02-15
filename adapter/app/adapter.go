package app

import (
	"crypto/tls"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/binding"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/ingress"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/timeoutwaitgroup"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/health"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// MetricClient is used to emit metrics.
type MetricClient interface {
	NewCounterMetric(string, ...pulseemitter.MetricOption) pulseemitter.CounterMetric
	NewGaugeMetric(string, string, ...pulseemitter.MetricOption) pulseemitter.GaugeMetric
}

// LogClient is used to emit logs.
type LogClient interface {
	EmitLog(message string, opts ...loggregator.EmitLogOption)
}

// Adapter receives bindings from the scheduler, connects to the RLP for log
// data, and streams it out to a syslog endpoint.
type Adapter struct {
	mu                sync.Mutex
	healthAddr        string
	adapterServerAddr string

	ctx    context.Context
	cancel func()

	adapterServer          *grpc.Server
	bindingManager         *binding.BindingManager
	maxBindings            int
	logsAPIConnCount       int
	logsAPIConnTTL         time.Duration
	logsEgressAPITLSConfig *tls.Config
	adapterServerTLSConfig *tls.Config
	syslogKeepalive        time.Duration
	syslogDialTimeout      time.Duration
	syslogIOTimeout        time.Duration
	skipCertVerify         bool
	health                 *health.Health
	timeoutWaitGroup       *timeoutwaitgroup.TimeoutWaitGroup
	sourceIndex            string
	metricsToSyslogEnabled bool
}

// AdapterOption is a type that will manipulate a config
type AdapterOption func(*Adapter)

// WithHealthAddr sets the address for the health endpoint to bind to.
func WithHealthAddr(addr string) AdapterOption {
	return func(c *Adapter) {
		c.healthAddr = addr
	}
}

// WithAdapterServerAddr sets the address for the gRPC server to bind to.
func WithAdapterServerAddr(addr string) AdapterOption {
	return func(c *Adapter) {
		c.adapterServerAddr = addr
	}
}

// WithMaxBindings sets the maximum bindings allowed per adapter.
func WithMaxBindings(i int) AdapterOption {
	return func(c *Adapter) {
		c.maxBindings = i
	}
}

// WithLogsEgressAPIConnCount sets the maximum number of connections to the
// Loggregator API
func WithLogsEgressAPIConnCount(m int) AdapterOption {
	return func(c *Adapter) {
		c.logsAPIConnCount = m
	}
}

// WithLogsEgressAPIConnTTL sets the number of seconds for a connection to the
// Loggregator API to live
func WithLogsEgressAPIConnTTL(d int) AdapterOption {
	return func(c *Adapter) {
		c.logsAPIConnTTL = time.Duration(int64(d)) * time.Second
	}
}

// WithSyslogKeepalive configures the keepalive interval for HTTPS, TCP, and
// TLS syslog drains.
func WithSyslogKeepalive(d time.Duration) AdapterOption {
	return func(a *Adapter) {
		a.syslogKeepalive = d
	}
}

// WithSyslogDialTimeout sets the TCP dial timeout for connecting to a syslog
// drain
func WithSyslogDialTimeout(d time.Duration) AdapterOption {
	return func(a *Adapter) {
		a.syslogDialTimeout = d
	}
}

// WithSyslogIOTimeout sets the TCP IO timeout for writing to a syslog
// drain
func WithSyslogIOTimeout(d time.Duration) AdapterOption {
	return func(a *Adapter) {
		a.syslogIOTimeout = d
	}
}

// WithSyslogSkipCertVerify sets the TCP InsecureSkipVerify property for
// syslog
func WithSyslogSkipCertVerify(b bool) AdapterOption {
	return func(a *Adapter) {
		a.skipCertVerify = b
	}
}

// WithEnableMetricsToSyslog returns a AdapterOption to override the
// default setting for writing metrics to syslog. By default this feature is
// disabled.
func WithMetricsToSyslogEnabled(enabled bool) AdapterOption {
	return func(a *Adapter) {
		a.metricsToSyslogEnabled = enabled
	}
}

// maxRetries for the backoff, results in around an hour of total delay
const maxRetries int = 22

// NewAdapter returns an Adapter
func NewAdapter(
	logsEgressAPIAddr string,
	logsEgressAPIAddrWithAZ string,
	logsEgressAPITLSConfig *tls.Config,
	adapterServerTLSConfig *tls.Config,
	metricClient MetricClient,
	logClient LogClient,
	sourceIndex string,
	opts ...AdapterOption,
) *Adapter {
	ctx, cancel := context.WithCancel(context.Background())

	a := &Adapter{
		healthAddr:             "127.0.0.1:8080",
		adapterServerAddr:      ":443",
		maxBindings:            500,
		ctx:                    ctx,
		cancel:                 cancel,
		logsAPIConnCount:       10,
		logsAPIConnTTL:         600 * time.Second,
		logsEgressAPITLSConfig: logsEgressAPITLSConfig,
		adapterServerTLSConfig: adapterServerTLSConfig,
		syslogDialTimeout:      5 * time.Second,
		syslogIOTimeout:        60 * time.Second,
		skipCertVerify:         true,
		health:                 health.NewHealth(),
		timeoutWaitGroup:       timeoutwaitgroup.New(time.Minute),
		sourceIndex:            sourceIndex,
		metricsToSyslogEnabled: false,
	}

	for _, o := range opts {
		o(a)
	}

	balancers := []ingress.Balancer{
		ingress.NewIPBalancer(logsEgressAPIAddrWithAZ),
		ingress.NewIPBalancer(logsEgressAPIAddr),
	}
	connector := ingress.NewConnector(
		balancers,
		5*time.Second,
		a.logsEgressAPITLSConfig,
	)
	clientManager := ingress.NewClientManager(
		connector,
		a.logsAPIConnCount,
		a.logsAPIConnTTL,
		time.Second,
	)

	constructors := map[string]egress.WriterConstructor{
		"https": egress.RetryWrapper(
			egress.NewHTTPSWriter,
			egress.ExponentialDuration,
			maxRetries,
			logClient,
			sourceIndex,
		),
		"syslog": egress.RetryWrapper(
			egress.NewTCPWriter,
			egress.ExponentialDuration,
			maxRetries,
			logClient,
			sourceIndex,
		),
		"syslog-tls": egress.RetryWrapper(
			egress.NewTLSWriter,
			egress.ExponentialDuration,
			maxRetries,
			logClient,
			sourceIndex,
		),
	}

	droppedMetric := buildMetric(metricClient, "dropped")
	droppedMetrics := map[string]pulseemitter.CounterMetric{
		// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
		// when sending to a syslog drain over https.
		"https": droppedMetric,
		// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
		// when sending to a syslog drain over syslog.
		"syslog": droppedMetric,
		// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
		// when sending to a syslog drain over syslog-tls.
		"syslog-tls": droppedMetric,
	}

	egressMetric := buildMetric(metricClient, "egress")
	egressMetrics := map[string]pulseemitter.CounterMetric{
		// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
		// to a syslog drain over https.
		"https": egressMetric,
		// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
		// to a syslog drain over syslog.
		"syslog": egressMetric,
		// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
		// to a syslog drain over syslog-tls.
		"syslog-tls": egressMetric,
	}

	syslogConnector := egress.NewSyslogConnector(
		egress.NetworkTimeoutConfig{
			Keepalive:    a.syslogKeepalive,
			DialTimeout:  a.syslogDialTimeout,
			WriteTimeout: a.syslogIOTimeout,
		},
		a.skipCertVerify,
		a.timeoutWaitGroup,
		egress.WithConstructors(constructors),
		egress.WithDroppedMetrics(droppedMetrics),
		egress.WithEgressMetrics(egressMetrics),
		egress.WithLogClient(logClient, a.sourceIndex),
	)
	subscriber := ingress.NewSubscriber(
		a.ctx,
		clientManager,
		syslogConnector,
		metricClient,
		ingress.WithLogClient(logClient, a.sourceIndex),
		ingress.WithMetricsToSyslogEnabled(a.metricsToSyslogEnabled),
	)

	a.bindingManager = binding.NewBindingManager(
		subscriber,
		metricClient,
		logClient,
		a.sourceIndex,
		binding.WithMaxBindings(a.maxBindings),
	)
	a.healthAddr = health.StartServer(a.health, a.healthAddr)

	return a
}

func buildMetric(m MetricClient, name string) pulseemitter.CounterMetric {
	return m.NewCounterMetric(
		name,
		pulseemitter.WithVersion(2, 0),
	)
}

// Start starts the adapter health endpoint and gRPC service.
func (a *Adapter) Start() error {
	lis, err := net.Listen("tcp", a.adapterServerAddr) // close this listener
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	kp := keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}

	adapterServer := binding.NewAdapterServer(a.bindingManager, a.health)
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(a.adapterServerTLSConfig)),
		grpc.KeepaliveEnforcementPolicy(kp),
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
