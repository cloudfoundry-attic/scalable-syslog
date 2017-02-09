package main

import (
	"flag"
	"log"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/api"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
)

var (
	healthHostport = flag.String("health", ":8080", "The hostport to listen for health requests")
	pprofHostport  = flag.String("pprof", ":6060", "The hostport to listen for pprof")
	cupsProvider   = flag.String("cups-url", "", "The URL of the CUPS provider")

	caFile         = flag.String("cups-ca", "", "The file path for the CA cert")
	certFile       = flag.String("cups-cert", "", "The file path for the client cert")
	keyFile        = flag.String("cups-key", "", "The file path for the client key")
	commonName     = flag.String("cups-cn", "", "The common name used for the TLS config")
	skipCertVerify = flag.Bool("cups-skip-cert-verify", false, "The option to allow insecure SSL connections")
)

func main() {
	flag.Parse()
	log.Print("Starting scheduler...")
	defer log.Print("Closing scheduler.")

	tlsConfig, err := api.NewMutualTLSConfig(*certFile, *keyFile, *caFile, *commonName)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}
	tlsConfig.InsecureSkipVerify = *skipCertVerify

	schedulerAddr := app.StartScheduler(
		app.WithHealthAddr(*healthHostport),
		app.WithCUPSUrl(*cupsProvider),
		app.WithHTTPClient(api.NewHTTPSClient(tlsConfig, 5*time.Second)),
	)
	log.Printf("Health endpoint is listening on %s", schedulerAddr)

	log.Println(http.ListenAndServe(*pprofHostport, nil))
}
