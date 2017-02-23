package main

import (
	"flag"
	"log"
	"net"

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

	rlpCaFile := flag.String("rlp-ca", "", "The file path for the Loggregator CA cert")
	rlpCertFile := flag.String("rlp-cert", "", "The file path for the adapter RLP client cert")
	rlpKeyFile := flag.String("rlp-key", "", "The file path for the adapter RLP client key")
	rlpCommonName := flag.String("rlp-cn", "", "The common name for the Loggregator egress API")

	logsApiAddr := flag.String("logs-api-addr", "", "The address for the logs API")
	flag.Parse()

	tlsConfig, err := api.NewMutualTLSConfig(*certFile, *keyFile, *caFile, *commonName)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}

	rlpTlsConfig, err := api.NewMutualTLSConfig(*rlpCertFile, *rlpKeyFile, *rlpCaFile, *rlpCommonName)
	if err != nil {
		log.Fatalf("Invalid RLP TLS config: %s", err)
	}

	adapter := app.NewAdapter(
		*logsApiAddr,
		rlpTlsConfig,
		tlsConfig,
		app.WithHealthAddr(*healthHostport),
		app.WithControllerAddr(*adapterHostport),
	)
	adapter.Start()

	lis, err := net.Listen("tcp", *pprofHostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
