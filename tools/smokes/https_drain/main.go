package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	handler := NewSyslog()
	go handler.reportCount()

	http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("PORT")), handler)
}

type Handler struct {
	count uint64
}

func NewSyslog() *Handler {
	return &Handler{}
}

func (h *Handler) reportCount() {
	url := os.Getenv("COUNTER_URL") + "/set"
	if url == "" {
		log.Fatalf("Missing COUNTER_URL environment variable")
	}

	for range time.Tick(time.Second) {
		newCount := atomic.LoadUint64(&h.count)
		log.Printf("Updating count on counter app to %d", newCount)

		countStr := fmt.Sprint(newCount)
		resp, err := http.Post(url, "text/plain", strings.NewReader(countStr))
		if err != nil {
			log.Printf("Failed to write count: %s", err)
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Failed to write count: expected 200 got %d", resp.StatusCode)
		}
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(body) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	atomic.AddUint64(&h.count, 1)

	return
}
