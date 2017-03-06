package app

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/health"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Scheduler represents the scheduler component. It is responsible for polling
// and/or streaming events from the cloud controller about syslog drains and
// updating a pool of adapters to service those drains.
type Scheduler struct {
	cupsURL          string
	adapterAddrs     []string
	adapterTLSConfig *tls.Config

	healthAddr string
	client     *http.Client
	interval   time.Duration

	bindingRepo *egress.BindingRepository
	fetcher     *ingress.VersionFilter
}

// NewScheduler returns a new unstarted scheduler.
func NewScheduler(
	cupsURL string,
	adapterAddrs []string,
	adapterTLSConfig *tls.Config,
	opts ...SchedulerOption,
) *Scheduler {
	s := &Scheduler{
		cupsURL:          cupsURL,
		adapterAddrs:     adapterAddrs,
		adapterTLSConfig: adapterTLSConfig,
		healthAddr:       ":8080",
		client:           http.DefaultClient,
		interval:         15 * time.Second,
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

// WithHTTPClient sets the http.Client to poll the CUPS provider.
func WithHTTPClient(client *http.Client) func(*Scheduler) {
	return func(s *Scheduler) {
		s.client = client
	}
}

// WithPollingInterval sets the interval to poll the CUPS provider.
func WithPollingInterval(interval time.Duration) func(*Scheduler) {
	return func(s *Scheduler) {
		s.interval = interval
	}
}

// Start starts polling the CUPS provider and serves the HTTP health endpoint.
func (s *Scheduler) Start() string {
	s.setupIngress()
	s.setupEgress()
	s.startEgress()
	return s.serveHealth()
}

func (s *Scheduler) setupIngress() {
	s.fetcher = ingress.NewVersionFilter(ingress.NewBindingFetcher(cupsHTTPClient{
		client: s.client,
		addr:   s.cupsURL,
	}))
}

func (s *Scheduler) setupEgress() {
	creds := credentials.NewTLS(s.adapterTLSConfig)

	pool := egress.NewAdapterPool(s.adapterAddrs, grpc.WithTransportCredentials(creds))
	s.bindingRepo = egress.NewBindingRepository(pool)
}

func (s *Scheduler) startEgress() {
	orchestrator := egress.NewOrchestrator(s.fetcher, s.bindingRepo)
	go orchestrator.Run(s.interval)
}

func (s *Scheduler) serveHealth() string {
	router := http.NewServeMux()
	router.Handle("/health", health.NewHealth(s.fetcher, s.bindingRepo))

	server := http.Server{
		Addr:         s.healthAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	server.Handler = router

	lis, err := net.Listen("tcp", s.healthAddr)
	if err != nil {
		log.Fatalf("Unable to setup Health endpoint (%s): %s", s.healthAddr, err)
	}

	go func() {
		log.Printf("Health endpoint is listening on %s", lis.Addr().String())
		log.Fatalf("Health server closing: %s", server.Serve(lis))
	}()
	return lis.Addr().String()
}

type cupsHTTPClient struct {
	client *http.Client
	addr   string
}

func (w cupsHTTPClient) Get(nextID int) (*http.Response, error) {
	return w.client.Get(fmt.Sprintf("%s?batch_size=50&next_id=%d", w.addr, nextID))
}
