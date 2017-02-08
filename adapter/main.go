package main

import (
	"flag"
	"log"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"
)

var healthHostport = flag.String("health", ":8080", "The hostport to listen for health requests")
var pprofHostport = flag.String("pprof", ":6060", "The hostport to listen for pprof")

func main() {
	flag.Parse()
	log.Print("Starting adapter...")
	defer log.Print("Closing adapter.")

	actualHostPort := app.StartAdapter(*healthHostport)
	log.Printf("Health endpoint is listening on %s", actualHostPort)

	log.Println(http.ListenAndServe(*pprofHostport, nil))
}
