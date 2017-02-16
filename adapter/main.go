package main

import (
	"flag"
	"log"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
	"github.com/cloudfoundry-incubator/scalable-syslog/api"
)

func main() {
	healthHostport := flag.String("health", ":8080", "The hostport to listen for health requests")
	adapterHostport := flag.String("addr", ":4443", "The hostport to for the adapter controller")
	pprofHostport := flag.String("pprof", "localhost:6060", "The hostport to listen for pprof")

	caFile := flag.String("ca", "", "The file path for the CA cert")
	certFile := flag.String("cert", "", "The file path for the adapter server cert")
	keyFile := flag.String("key", "", "The file path for the adapter server key")
	commonName := flag.String("cn", "", "The common name used for the TLS config")

	logsApiAddr := flag.String("logs-api-addr", "", "The address for the logs API")
	flag.Parse()

	tlsConfig, err := api.NewMutualTLSConfig(*certFile, *keyFile, *caFile, *commonName)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}

	app.StartAdapter(
		app.WithHealthAddr(*healthHostport),
		app.WithControllerAddr(*adapterHostport),
		app.WithControllerTLSConfig(tlsConfig),
		app.WithLogsEgressAPIAddr(*logsApiAddr),
	)

	log.Println(http.ListenAndServe(*pprofHostport, nil))
}
