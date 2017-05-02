package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"net/http"
	_ "net/http/pprof"

	"code.cloudfoundry.org/scalable-syslog/adapter/app"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"
)

func main() {
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

	// metric-documentation-v2: setup function
	metricClient, err := metricemitter.NewClient(
		cfg.MetricIngressAddr,
		metricemitter.WithGRPCDialOptions(grpc.WithTransportCredentials(credentials.NewTLS(metricIngressTLS))),
		metricemitter.WithOrigin("scalablesyslog.adapter"),
		metricemitter.WithPulseInterval(cfg.MetricEmitterInterval),
	)
	if err != nil {
		log.Fatalf("Couldn't connect to metric emitter: %s", err)
	}

	adapter := app.NewAdapter(
		cfg.LogsAPIAddr,
		rlpTlsConfig,
		tlsConfig,
		metricClient,
		app.WithHealthAddr(cfg.HealthHostport),
		app.WithAdapterServerAddr(cfg.AdapterHostport),
		app.WithSyslogDialTimeout(cfg.SyslogDialTimeout),
		app.WithSyslogIOTimeout(cfg.SyslogIOTimeout),
		app.WithSyslogSkipCertVerify(cfg.SyslogSkipCertVerify),
	)
	adapter.Start()

	lis, err := net.Listen("tcp", cfg.PprofHostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
