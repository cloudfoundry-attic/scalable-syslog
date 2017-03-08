package main

import (
	"log"
	"net"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/api"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/config"
)

func main() {
	cfg := config.Load()

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

	scheduler := app.NewScheduler(
		cfg.APIURL,
		cfg.AdapterAddrs,
		adapterTLSConfig,
		app.WithHealthAddr(cfg.HealthHostport),
		app.WithHTTPClient(api.NewHTTPSClient(apiTLSConfig, 5*time.Second)),
	)
	scheduler.Start()

	lis, err := net.Listen("tcp", cfg.PprofHostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
