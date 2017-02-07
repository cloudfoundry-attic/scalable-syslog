package main

import (
	"flag"
	"log"

	"net/http"
	_ "net/http/pprof"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
)

var healthHostport = flag.String("health-hostport", ":8080", "The hostport to listen for health requests")
var pprofHostport = flag.String("pprof-hostport", ":6060", "The hostport to listen for pprof")

func main() {
	flag.Parse()
	log.Print("Starting scheduler...")
	defer log.Print("Closing scheduler.")

	actualHostPort := app.StartScheduler(*healthHostport)
	log.Printf("Health endpoint is listening on %s", actualHostPort)

	log.Println(http.ListenAndServe(*pprofHostport, nil))
}
