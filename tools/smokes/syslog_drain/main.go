package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/crewjam/rfc5424"
)

var count uint64

func main() {
	l, err := net.Listen("tcp", fmt.Sprintf(":%s", os.Getenv("PORT")))
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log.Print("Listening on " + os.Getenv("PORT"))

	go reportCount()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting: %s", err)
			continue
		}

		go handleRequest(conn)
	}
}

func reportCount() {
	url := os.Getenv("COUNTER_URL") + "/set"
	if url == "" {
		log.Fatalf("Missing COUNTER_URL environment variable")
	}

	for range time.Tick(time.Second) {
		newCount := atomic.LoadUint64(&count)
		log.Printf("Updating count on counter app to %d", newCount)

		countStr := fmt.Sprint(newCount)
		resp, err := http.Post(url, "text/plain", strings.NewReader(countStr))
		if err != nil {
			log.Println("Failed to write count: %s", err)
		}

		if resp.StatusCode != http.StatusOK {
			log.Println("Failed to write count: expected 200 got %d", resp.StatusCode)
		}
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	var msg rfc5424.Message
	for {
		_, err := msg.ReadFrom(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("ReadFrom err: %s", err)
			break
		}

		atomic.AddUint64(&count, 1)
	}
}
