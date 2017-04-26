package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"

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

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting: %s", err)
			continue
		}

		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	var msg rfc5424.Message
	for {
		_, err := msg.ReadFrom(conn)
		if err != nil {
			log.Printf("ReadFrom err: %s", err)
			break
		}

		atomic.AddUint64(&count, 1)
	}
	_, err := conn.Write([]byte(fmt.Sprintf("HTTP/1.1 200 OK\nLocation: http://who.cares.com\nContent-Type: application/json; charset=UTF-8\n\n%d", atomic.LoadUint64(&count))))
	if err != nil {
		log.Printf("Write err: %s", err)
	}
	conn.Close()
}
