package main

import (
	"flag"
	"log"
	"math/rand"
	"net"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/api"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
)

func main() {
	rand.Seed(time.Now().Unix())

	healthHostport := flag.String("health", ":8080", "The hostport to listen for health requests")
	pprofHostport := flag.String("pprof", ":6060", "The hostport to listen for pprof")

	cupsProvider := flag.String("cups-url", "", "The URL of the CUPS provider")
	cupsCAFile := flag.String("cups-ca", "", "The file path for the CA cert")
	cupsCertFile := flag.String("cups-cert", "", "The file path for the client cert")
	cupsKeyFile := flag.String("cups-key", "", "The file path for the client key")
	cupsCommonName := flag.String("cups-cn", "", "The common name used for the TLS config")
	skipCertVerify := flag.Bool("cups-skip-cert-verify", false, "The option to allow insecure SSL connections")

	caFile := flag.String("ca", "", "The file path for the CA cert")
	certFile := flag.String("cert", "", "The file path for the adapter server cert")
	keyFile := flag.String("key", "", "The file path for the adapter server key")

	adapterCommonName := flag.String("adapter-cn", "", "The common name used for the TLS config")
	adapterIPs := flag.String("adapter-ips", "", "Comma separated list of adapter IP addresses")
	adapterPort := flag.String("adapter-port", "", "The port of the adapter API")

	flag.Parse()

	adapterAddrs, err := app.ParseAddrs(*adapterIPs, *adapterPort)
	if err != nil {
		log.Fatalf("No adapter addresses: %s", err)
	}

	cupsTLSConfig, err := api.NewMutualTLSConfig(*cupsCertFile, *cupsKeyFile, *cupsCAFile, *cupsCommonName)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}
	cupsTLSConfig.InsecureSkipVerify = *skipCertVerify

	adapterTLSConfig, err := api.NewMutualTLSConfig(*certFile, *keyFile, *caFile, *adapterCommonName)
	if err != nil {
		log.Fatalf("Invalid TLS config: %s", err)
	}

	scheduler := app.NewScheduler(
		*cupsProvider,
		adapterAddrs,
		adapterTLSConfig,
		app.WithHealthAddr(*healthHostport),
		app.WithHTTPClient(api.NewHTTPSClient(cupsTLSConfig, 5*time.Second)),
	)
	scheduler.Start()

	lis, err := net.Listen("tcp", *pprofHostport)
	if err != nil {
		log.Printf("Error creating pprof listener: %s", err)
	}

	log.Printf("Starting pprof server on: %s", lis.Addr().String())
	log.Println(http.Serve(lis, nil))
}
