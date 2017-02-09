package main

import (
	"flag"
	"log"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
	"github.com/cloudfoundry-incubator/scalable-syslog/api"
)

var (
	healthHostport  = flag.String("health", ":8080", "The hostport to listen for health requests")
	adapterHostport = flag.String("addr", ":4443", "The hostport to for the adapter controller")
	pprofHostport   = flag.String("pprof", "localhost:6060", "The hostport to listen for pprof")

	caFile     = flag.String("ca", "", "The file path for the CA cert")
	certFile   = flag.String("cert", "", "The file path for the adapter server cert")
	keyFile    = flag.String("key", "", "The file path for the adapter server key")
	commonName = flag.String("cn", "", "The common name used for the TLS config")
)

func main() {
	flag.Parse()
	log.Print("Starting adapter...")
	defer log.Print("Closing adapter.")

	tlsConfig, err := api.NewMutualTLSConfig(*certFile, *keyFile, *caFile, *commonName)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}

	healthHostport, controllerHealthport := app.StartAdapter(
		app.WithHealthAddr(*healthHostport),
		app.WithControllerAddr(*adapterHostport),
		app.WithControllerTLSConfig(tlsConfig),
	)
	log.Printf("Health endpoint is listening on %s", healthHostport)
	log.Printf("Adapter controller is listening on %s", controllerHealthport)

	log.Println(http.ListenAndServe(*pprofHostport, nil))
}
