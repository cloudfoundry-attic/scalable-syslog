package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"net/http"
	_ "net/http/pprof"

	"code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/scalable-syslog/adapter/app"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := app.LoadConfig()

	tlsConfig, err := api.NewMutualTLSConfig(
		cfg.CertFile,
		cfg.KeyFile,
		cfg.CAFile,
		cfg.CommonName,
	)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}

	rlpTlsConfig, err := api.NewMutualTLSConfig(
		cfg.RLPCertFile,
		cfg.RLPKeyFile,
		cfg.RLPCAFile,
		cfg.RLPCommonName,
	)
	if err != nil {
		log.Fatalf("Invalid RLP TLS config: %s", err)
	}

	metricIngressTLS, err := api.NewMutualTLSConfig(
		cfg.RLPCertFile,
		cfg.RLPKeyFile,
		cfg.RLPCAFile,
		cfg.MetricIngressCN,
	)
	if err != nil {
		log.Fatalf("Invalid Metric Ingress TLS config: %s", err)
	}

	logClient, err := loggregator.NewIngressClient(
		metricIngressTLS,
		loggregator.WithTag("origin", "cf-syslog-drain.adapter"),
		loggregator.WithAddr(cfg.MetricIngressAddr),
	)
	if err != nil {
		log.Fatalf("Couldn't connect to metric ingress server: %s", err)
	}

	metricClient := pulseemitter.New(
		logClient,
		pulseemitter.WithPulseInterval(cfg.MetricEmitterInterval),
		pulseemitter.WithSourceID("drain_adapter"),
	)

	go startPprof(cfg.PprofHostport)

	adapter := app.NewAdapter(
		cfg.LogsAPIAddr,
		cfg.LogsAPIAddrWithAZ,
		rlpTlsConfig,
		tlsConfig,
		metricClient,
		logClient,
		cfg.SourceIndex,
		app.WithHealthAddr(cfg.HealthHostport),
		app.WithAdapterServerAddr(cfg.AdapterHostport),
		app.WithSyslogKeepalive(cfg.SyslogKeepalive),
		app.WithSyslogDialTimeout(cfg.SyslogDialTimeout),
		app.WithSyslogIOTimeout(cfg.SyslogIOTimeout),
		app.WithSyslogSkipCertVerify(cfg.SyslogSkipCertVerify),
		app.WithMetricsToSyslogEnabled(cfg.MetricsToSyslogEnabled),
		app.WithMaxBindings(cfg.MaxBindings),
	)
	go adapter.Start()
	defer adapter.Stop()

	killSignal := make(chan os.Signal, 1)
	signal.Notify(killSignal, syscall.SIGINT, syscall.SIGTERM)
	<-killSignal
}

func startPprof(hostport string) {
	lis, err := net.Listen("tcp", hostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
