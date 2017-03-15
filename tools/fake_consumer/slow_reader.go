package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "addr to listen on")
	delay := flag.Duration("delay", time.Second, "amount of time to wait between reads")
	flag.Parse()

	log.Printf("listening on %s", *addr)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := lis.Accept()
		log.Print("accepted connection")
		if err != nil {
			log.Printf("error with accepting connection: %s", err)
			continue
		}
		go handleConn(conn, *delay)
	}
}

func handleConn(conn net.Conn, delay time.Duration) {
	buf := bufio.NewReader(conn)
	for {
		log.Print("reading from connection")
		data, err := buf.ReadString('\n')
		log.Printf("read from connection: %s", string(data))
		if err != nil {
			log.Printf("error with reading from connection: %s", err)
			return
		}
		time.Sleep(delay)
	}
}
