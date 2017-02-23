package app

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/controller"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/drainstore"
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

// StartAdapter starts the health endpoint and gRPC service.
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
	}

	for _, o := range opts {
		o(adapter)
	}

	return adapter
}

func (a *Adapter) Start() (actualHealth, actualService string) {
	log.Print("Starting adapter...")

	cache := drainstore.NewCache()

	actualHealth = startHealthServer(a.healthAddr, cache)
	creds := credentials.NewTLS(a.controllerTLSConfig)
	actualService = startAdapterService(a.controllerAddr, creds, cache)

	connector := ingress.NewConnector(
		a.logsEgressAPIAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(a.logsEgressAPITLSConfig)),
	)
	ingress.NewConsumer(connector, a.logsAPIConnCount, a.logsAPIConnTTL)

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
	router.Handle("/health", health.NewHealth(cache))
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

