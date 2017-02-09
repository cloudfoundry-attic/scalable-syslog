package app

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/cups"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/drainstore"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/handlers"
)

// StartScheduler starts polling the CUPS provider and serves the HTTP
// health endpoint.
func StartScheduler(opts ...SchedulerOption) (actualHealth string) {
	conf := setupConfig(opts)

	l, err := net.Listen("tcp", conf.healthAddr)
	if err != nil {
		log.Fatalf("Unable to setup Health endpoint (%s): %s", conf.healthAddr, err)
	}

	server := http.Server{
		Addr:         conf.healthAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	client := clientWrapper{
		client: conf.client,
		addr:   conf.cupsURL,
	}

	store := drainstore.NewCache()
	fetcher := cups.NewBindingFetcher(client)
	cups.StartPoller(conf.interval, fetcher, store)

	router := http.NewServeMux()
	router.Handle("/health", handlers.NewHealth(store))

	server.Handler = router

	go func() {
		log.Fatalf("Health server closing: %s", server.Serve(l))
	}()

	return l.Addr().String()
}

// SchedulerOption is a type that will manipulate a config
type SchedulerOption func(c *config)

// WithHealthAddr sets the address for the health endpoint to bind to.
func WithHealthAddr(addr string) func(*config) {
	return func(c *config) {
		c.healthAddr = addr
	}
}

// WithCUPSUrl is the endpoint of the CUPS provider
func WithCUPSUrl(URL string) func(*config) {
	return func(c *config) {
		c.cupsURL = URL
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
	cupsURL    string
	client     *http.Client
	interval   time.Duration
}

func setupConfig(opts []SchedulerOption) *config {
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

type clientWrapper struct {
	client *http.Client
	addr   string
}

func (w clientWrapper) Get(nextID int) (*http.Response, error) {
	return w.client.Get(fmt.Sprintf("%s?batch_size=50&next_id=%d", w.addr, nextID))
}
