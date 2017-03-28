package health

import (
	"log"
	"net"
	"net/http"
	"time"
)

func StartServer(h *Health, addr string) string {
	router := http.NewServeMux()
	router.Handle("/health", h)

	server := http.Server{
		Addr:         addr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	server.Handler = router

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Unable to setup Health endpoint (%s): %s", addr, err)
	}

	go func() {
		log.Printf("Health endpoint is listening on %s", lis.Addr().String())
		log.Fatalf("Health server closing: %s", server.Serve(lis))
	}()
	return lis.Addr().String()
}
