package app

import (
	"crypto/tls"
	"net/http"
	"time"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/scalable-syslog/internal/health"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Scheduler represents the scheduler component. It is responsible for polling
// and/or streaming events from the cloud controller about syslog drains and
// updating a pool of adapters to service those drains.
type Scheduler struct {
	apiURL           string
	adapterAddrs     []string
	adapterTLSConfig *tls.Config
	healthAddr       string
	health           *health.Health
	emitter          Emitter
	client           *http.Client
	interval         time.Duration
	adapterService   *egress.AdapterService
	fetcher          *ingress.FilteredBindingFetcher
	logClient        LogClient
	blacklist        *ingress.BlacklistRanges
}

// Emitter sends gauge metrics
type Emitter interface {
	NewGaugeMetric(name, unit string, opts ...pulseemitter.MetricOption) pulseemitter.GaugeMetric
}

// LogClient is used to emit logs.
type LogClient interface {
	EmitLog(message string, opts ...loggregator.EmitLogOption)
}

// NewScheduler returns a new unstarted scheduler.
func NewScheduler(
	apiURL string,
	adapterAddrs []string,
	adapterTLSConfig *tls.Config,
	e Emitter,
	logClient LogClient,
	opts ...SchedulerOption,
) *Scheduler {
	s := &Scheduler{
		apiURL:           apiURL,
		adapterAddrs:     adapterAddrs,
		adapterTLSConfig: adapterTLSConfig,
		healthAddr:       ":8080",
		client:           http.DefaultClient,
		interval:         15 * time.Second,
		blacklist:        &ingress.BlacklistRanges{},
		health:           health.NewHealth(),
		logClient:        logClient,
		emitter:          e,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// SchedulerOption represents a function that can configure a scheduler.
type SchedulerOption func(c *Scheduler)

// WithHealthAddr sets the address for the health endpoint to bind to.
func WithHealthAddr(addr string) func(*Scheduler) {
	return func(s *Scheduler) {
		s.healthAddr = addr
	}
}

// WithHTTPClient sets the http.Client to poll the syslog drain binding provider.
func WithHTTPClient(client *http.Client) func(*Scheduler) {
	return func(s *Scheduler) {
		s.client = client
	}
}

// WithPollingInterval sets the interval to poll the syslog drain binding provider.
func WithPollingInterval(interval time.Duration) func(*Scheduler) {
	return func(s *Scheduler) {
		s.interval = interval
	}
}

// WithBlacklist sets the blacklist for the syslog IPs.
func WithBlacklist(r *ingress.BlacklistRanges) func(*Scheduler) {
	return func(s *Scheduler) {
		s.blacklist = r
	}
}

// Start starts polling the syslog drain binding provider and serves the HTTP
// health endpoint.
func (s *Scheduler) Start() string {
	s.setupIngress()
	s.startEgress()
	return s.serveHealth()
}

func (s *Scheduler) setupIngress() {
	var fetcher ingress.BindingReader

	fetcher = ingress.NewBindingFetcher(
		ingress.APIClient{
			Client: s.client,
			Addr:   s.apiURL,
		},
	)

	s.fetcher = ingress.NewFilteredBindingFetcher(s.blacklist, fetcher, s.logClient)
}

func (s *Scheduler) startEgress() {
	creds := credentials.NewTLS(s.adapterTLSConfig)

	kp := keepalive.ClientParameters{
		Time:                15 * time.Second,
		Timeout:             15 * time.Second,
		PermitWithoutStream: true,
	}

	pool := egress.NewAdapterPool(s.adapterAddrs, s.health,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(kp),
	)
	s.adapterService = egress.NewAdapterService(pool, pool)
	orchestrator := egress.NewOrchestrator(s.adapterAddrs, s.fetcher, s.adapterService, s.health, s.emitter)
	go orchestrator.Run(s.interval)
}

func (s *Scheduler) serveHealth() string {
	return health.StartServer(s.health, s.healthAddr)
}
