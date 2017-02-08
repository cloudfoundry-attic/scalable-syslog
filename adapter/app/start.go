package app

import (
	"log"
	"net"
	"net/http"
	"time"
)

func StartAdapter(healthHostport string) (actualHealth string) {
	l, err := net.Listen("tcp", healthHostport)
	if err != nil {
		log.Fatalf("Unable to setup Health endpoint (%s): %s", healthHostport, err)
	}

	server := http.Server{
		Addr:         healthHostport,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"drainCount": 0}`))
	})

	go func() {
		log.Fatalf("Health server closing: %s", server.Serve(l))
	}()

	return l.Addr().String()
}
