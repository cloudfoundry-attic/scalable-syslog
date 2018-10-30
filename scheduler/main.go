package main

import (
	"log"
	"net"
	"os"
	"time"

	"net/http"
	_ "net/http/pprof"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
	"code.cloudfoundry.org/scalable-syslog/scheduler/app"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

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
		cfg.CertFile,
		cfg.KeyFile,
		cfg.CAFile,
		cfg.MetricIngressCN,
	)
	if err != nil {
		log.Fatalf("Invalid Metric Ingress TLS config: %s", err)
	}

	logClient, err := loggregator.NewIngressClient(
		metricIngressTLS,
		loggregator.WithTag("origin", "cf-syslog-drain.scheduler"),
		loggregator.WithAddr(cfg.MetricIngressAddr),
	)
	if err != nil {
		log.Fatalf("Couldn't connect to metric ingress server: %s", err)
	}

	// metric-documentation-v2: setup function
	metricClient := pulseemitter.New(
		logClient,
		pulseemitter.WithPulseInterval(cfg.MetricEmitterInterval),
		pulseemitter.WithSourceID("drain_scheduler"),
	)

	scheduler := app.NewScheduler(
		cfg.APIURL,
		cfg.AdapterAddrs,
		adapterTLSConfig,
		metricClient,
		logClient,
		app.WithHealthAddr(cfg.HealthHostport),
		app.WithHTTPClient(api.NewHTTPSClient(apiTLSConfig, 5*time.Second)),
		app.WithBlacklist(cfg.Blacklist),
		app.WithPollingInterval(cfg.APIPollingInterval),
		app.WithAPIBatchSize(cfg.APIBatchSize),
	)
	scheduler.Start()

	lis, err := net.Listen("tcp", cfg.PprofHostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
