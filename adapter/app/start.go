package app

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/controller"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/drainstore"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/handlers"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/ingress"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// StartAdapter starts the health endpoint and gRPC service.
func StartAdapter(opts ...AdapterOption) (actualHealth, actualService string) {
	log.Print("Starting adapter...")
	conf := setupConfig(opts)

	cache := drainstore.NewCache()

	actualHealth = startHealthServer(conf.healthAddr, cache)
	actualService = startAdapterService(conf.controllerAddr, conf.controllerCreds, cache)

	connector := ingress.NewConnector(
		conf.logsAPIAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(conf.logsAPITLSConfig)),
	)
	ingress.NewConsumer(connector, conf.logsAPIConnCount, conf.logsAPIConnTTL)

	return actualHealth, actualService
}

func startHealthServer(hostport string, cache *drainstore.Cache) string {
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
	router.Handle("/health", handlers.NewHealth(cache))
	server.Handler = router

	go func() {
		log.Fatalf("Health server closing: %s", server.Serve(l))
	}()

	log.Printf("Health endpoint is listening on %s", l.Addr().String())
	return l.Addr().String()
}

func startAdapterService(hostport string, creds credentials.TransportCredentials, cache *drainstore.Cache) string {
	lis, err := net.Listen("tcp", hostport)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	adapterService := controller.New(cache)
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

// AdapterOption is a type that will manipulate a config
type AdapterOption func(c *config)

// WithHealthAddr sets the address for the health endpoint to bind to.
func WithHealthAddr(addr string) func(*config) {
	return func(c *config) {
		c.healthAddr = addr
	}
}

// WithServiceAddr sets the address for the gRPC controller to bind to.
func WithControllerAddr(addr string) func(*config) {
	return func(c *config) {
		c.controllerAddr = addr
	}
}

// WithServiceTLSConfig sets the TLS config for the adapter TLS mutual auth.
func WithControllerTLSConfig(cfg *tls.Config) func(*config) {
	return func(c *config) {
		c.controllerCreds = credentials.NewTLS(cfg)
	}
}

// WithLogsEgressAPIAddr sets the address for Loggregator Egress API
func WithLogsEgressAPIAddr(addr string) func(*config) {
	return func(c *config) {
		c.logsAPIAddr = addr
	}
}

// WithLogsEgressAPIConnCount sets the maximum number of connections to the
// Loggregator API
func WithLogsEgressAPIConnCount(m int) func(*config) {
	return func(c *config) {
		c.logsAPIConnCount = m
	}
}

// WithLogsEgressAPIConnTTL sets the number of seconds for a connection to the
// Loggregator API to live
func WithLogsEgressAPIConnTTL(d int) func(*config) {
	return func(c *config) {
		c.logsAPIConnTTL = time.Duration(int64(d)) * time.Second
	}
}

// WithLogsEgressAPITLSConfig sets the TLS mutual auth config for communication with adapter.
func WithLogsEgressAPITLSConfig(cfg *tls.Config) func(*config) {
	return func(c *config) {
		c.logsAPITLSConfig = cfg
	}
}

type config struct {
	healthAddr       string
	logsAPIAddr      string
	logsAPIConnCount int
	logsAPITLSConfig *tls.Config
	logsAPIConnTTL   time.Duration
	controllerAddr   string
	controllerCreds  credentials.TransportCredentials
}

func setupConfig(opts []AdapterOption) *config {
	conf := config{
		healthAddr:       ":8080",
		controllerAddr:   ":443",
		logsAPIConnCount: 3,
		logsAPIConnTTL:   600 * time.Second,
	}

	for _, o := range opts {
		o(&conf)
	}

	return &conf
}
