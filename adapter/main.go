package main

import (
	"log"
	"net"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/config"
	"github.com/cloudfoundry-incubator/scalable-syslog/api"
)

func main() {
	cfg := config.Load()

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

	adapter := app.NewAdapter(
		cfg.LogsAPIAddr,
		rlpTlsConfig,
		tlsConfig,
		app.WithHealthAddr(cfg.HealthHostport),
		app.WithControllerAddr(cfg.AdapterHostport),
		app.WithSyslogDialTimeout(cfg.SyslogDialTimeout),
		app.WithSyslogIOTimeout(cfg.SyslogIOTimeout),
	)
	adapter.Start()

	lis, err := net.Listen("tcp", cfg.PprofHostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
