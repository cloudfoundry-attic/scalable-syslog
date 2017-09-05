package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/rfc5424"
)

var (
	// primeCount will track the primer message count
	primeCount uint64

	// msgCount will track the actual message count
	msgCount uint64
)

func main() {
	l, err := net.Listen("tcp", fmt.Sprintf(":%s", os.Getenv("PORT")))
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log.Print("Listening on " + os.Getenv("PORT"))

	if os.Getenv("COUNTER_URL") != "" {
		go reportCount()
	}

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
		newPrimeCount := atomic.LoadUint64(&primeCount)
		newMsgCount := atomic.LoadUint64(&msgCount)
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

		fmt.Printf("%s\n", msg.Message)
		if !bytes.Contains(msg.Message, []byte("HTTP")) {
			if bytes.Contains(msg.Message, []byte("prime")) {
				atomic.AddUint64(&primeCount, 1)
			}
			if bytes.Contains(msg.Message, []byte("live")) {
				atomic.AddUint64(&msgCount, 1)
			}
		}
	}
}
