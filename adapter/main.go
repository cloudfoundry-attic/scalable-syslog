package main

import (
	"flag"
	"log"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
)

var healthHostport = flag.String("health", ":8080", "The hostport to listen for health requests")
var adapterHostport = flag.String("adapter", ":443", "The hostport to for the adapter service")
var pprofHostport = flag.String("pprof", ":6060", "The hostport to listen for pprof")

func main() {
	flag.Parse()
	log.Print("Starting adapter...")
	defer log.Print("Closing adapter.")

	healthHostport, serviceHealthport := app.StartAdapter(
		app.WithHealthAddr(*healthHostport),
		app.WithServiceAddr(*adapterHostport),
	)
	log.Printf("Health endpoint is listening on %s", healthHostport)
	log.Printf("Adapter service is listening on %s", serviceHealthport)

	log.Println(http.ListenAndServe(*pprofHostport, nil))
}
