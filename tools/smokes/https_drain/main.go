package main

import (
	"bytes"
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
	// primeCount will track the primer message count
	primeCount uint64

	// msgCount will track the actual message count
	msgCount uint64
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
		newPrimeCount := atomic.LoadUint64(&h.primeCount)
		newMsgCount := atomic.LoadUint64(&h.msgCount)
		log.Printf("Updating prime count: %d msg count: %d", newPrimeCount, newMsgCount)

		countStr := fmt.Sprintf("%d:%d", newPrimeCount, newMsgCount)
		resp, err := http.Post(url, "text/plain", strings.NewReader(countStr))
		if err != nil {
			log.Printf("Failed to write count: %s", err)
			continue
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

	if !bytes.Contains(body, []byte("HTTP")) {
		if bytes.Contains(body, []byte("prime")) {
			atomic.AddUint64(&h.primeCount, 1)
		}
		if bytes.Contains(body, []byte("live")) {
			atomic.AddUint64(&h.msgCount, 1)
		}
	}

	return
}
