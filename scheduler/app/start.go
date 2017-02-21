package app

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Start starts polling the CUPS provider and serves the HTTP
// health endpoint.
func Start(cupsURL string, adapterAddrs []string, adapterTLSConfig *tls.Config, opts ...AppOption) (actualHealth string) {
	log.Print("Starting scheduler...")

	conf := setupConfig(opts)

	client := cupsHTTPClient{
		client: conf.client,
		addr:   cupsURL,
	}
	fetcher := NewVersionFilter(NewBindingFetcher(client))

	creds := credentials.NewTLS(adapterTLSConfig)
	pool := NewAdapterWriterPool(
		adapterAddrs,
		grpc.WithTransportCredentials(creds),
	)
	orchestrator := NewOrchestrator(fetcher, pool)
	go orchestrator.Run(conf.interval)

	router := http.NewServeMux()
	router.Handle("/health", NewHealth(fetcher, pool))

	server := http.Server{
		Addr:         conf.healthAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	server.Handler = router

	l, err := net.Listen("tcp", conf.healthAddr)
	if err != nil {
		log.Fatalf("Unable to setup Health endpoint (%s): %s", conf.healthAddr, err)
	}

	go func() {
		log.Printf("Health endpoint is listening on %s", l.Addr().String())
		log.Fatalf("Health server closing: %s", server.Serve(l))
	}()

	return l.Addr().String()
}

// AppOption is a type that will manipulate a config
type AppOption func(c *config)

// WithHealthAddr sets the address for the health endpoint to bind to.
func WithHealthAddr(addr string) func(*config) {
	return func(c *config) {
		c.healthAddr = addr
	}
}

// WithHTTPClient sets the http.Client to poll the CUPS provider
func WithHTTPClient(client *http.Client) func(*config) {
	return func(c *config) {
		c.client = client
	}
}

// WithPollingInterval sets the interval to poll the CUPS provider
func WithPollingInterval(interval time.Duration) func(*config) {
	return func(c *config) {
		c.interval = interval
	}
}

type config struct {
	healthAddr string
	client     *http.Client
	interval   time.Duration
}

func setupConfig(opts []AppOption) *config {
	conf := config{
		healthAddr: ":8080",
		client:     http.DefaultClient,
		interval:   15 * time.Second,
	}

	for _, o := range opts {
		o(&conf)
	}

	return &conf
}

type cupsHTTPClient struct {
	client *http.Client
	addr   string
}

func (w cupsHTTPClient) Get(nextID int) (*http.Response, error) {
	return w.client.Get(fmt.Sprintf("%s?batch_size=50&next_id=%d", w.addr, nextID))
}
