package main

import (
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/internal/api"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/metric"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
)

func main() {
	cfg, err := app.LoadConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("Invalid config: %s", err)
	}

	apiTLSConfig, err := api.NewMutualTLSConfig(
		cfg.APICertFile,
		cfg.APIKeyFile,
		cfg.APICAFile,
		cfg.APICommonName,
	)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}
	apiTLSConfig.InsecureSkipVerify = cfg.APISkipCertVerify

	adapterTLSConfig, err := api.NewMutualTLSConfig(
		cfg.CertFile,
		cfg.KeyFile,
		cfg.CAFile,
		cfg.AdapterCommonName,
	)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}

	metricIngressTLS, err := api.NewMutualTLSConfig(
		cfg.MetricIngressCertFile,
		cfg.MetricIngressKeyFile,
		cfg.MetricIngressCAFile,
		cfg.MetricIngressCN,
	)
	if err != nil {
		log.Fatalf("Invalid Metric Ingress TLS config: %s", err)
	}

	emitter, err := metric.New(
		metric.WithGrpcDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(metricIngressTLS))),
		metric.WithOrigin("scalablesyslog.scheduler"),
		metric.WithAddr(cfg.MetricIngressAddr),
	)
	if err != nil {
		log.Printf("Failed to connect to metric ingress: %s", err)
	}

	scheduler := app.NewScheduler(
		cfg.APIURL,
		cfg.AdapterAddrs,
		adapterTLSConfig,
		emitter,
		app.WithOptIn(cfg.RequireOptIn),
		app.WithHealthAddr(cfg.HealthHostport),
		app.WithHTTPClient(api.NewHTTPSClient(apiTLSConfig, 5*time.Second)),
		app.WithBlacklist(cfg.Blacklist),
		app.WithPollingInterval(cfg.APIPollingInterval),
	)
	scheduler.Start()

	lis, err := net.Listen("tcp", cfg.PprofHostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
