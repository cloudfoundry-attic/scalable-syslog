// fake_scheduler: a program to put scheduling load on adapters.
//
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"code.cloudfoundry.org/scalable-syslog/internal/api"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	addr := flag.String("addr", ":8082", "The address of adapter to make requests at")
	caFile := flag.String("ca", "", "The file path to the CA file")
	certFile := flag.String("cert", "", "The server TLS cert")
	keyFile := flag.String("key", "", "The server TLS private key")
	commonName := flag.String("cn", "", "The server common name for TLS")
	lifetime := flag.Duration("lifetime", time.Second, "The time the binding is alive for before being deleted")
	delay := flag.Duration("delay", time.Second, "The time the server waits between creating and deleting bindings")

	flag.Parse()

	c, conn := client(*addr, *certFile, *keyFile, *caFile, *commonName)
	defer conn.Close()

	kill := make(chan os.Signal, 1)
	signal.Notify(kill, os.Interrupt)
	var wg sync.WaitGroup

loop:
	for i := 0; ; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			createBinding(c, i)
			time.Sleep(*lifetime)
			deleteBinding(c, i)
		}(i)
		time.Sleep(*delay)
		select {
		case <-kill:
			log.Print("kill signal received...")
			break loop
		default:
		}
	}
	log.Print("waiting for lifetimes to expire...")
	wg.Wait()
}

func createBinding(c v1.AdapterClient, i int) {
	_, err := c.CreateBinding(context.TODO(), &v1.CreateBindingRequest{
		Binding: &v1.Binding{
			AppId:    fmt.Sprintf("some-app-id-%d", i),
			Hostname: fmt.Sprintf("some.app.hostname-%d", i),
			Drain:    "syslog://localhost:12346",
		},
	})
	if err != nil {
		log.Print(err)
	}
}

func deleteBinding(c v1.AdapterClient, i int) {
	_, err := c.DeleteBinding(context.TODO(), &v1.DeleteBindingRequest{
		Binding: &v1.Binding{
			AppId:    fmt.Sprintf("some-app-id-%d", i),
			Hostname: fmt.Sprintf("some.app.hostname-%d", i),
			Drain:    "syslog://localhost:12346",
		},
	})
	if err != nil {
		log.Print(err)
	}
}

func client(addr, certFile, keyFile, caFile, commonName string) (v1.AdapterClient, io.Closer) {
	tlsConfig, err := api.NewMutualTLSConfig(
		certFile,
		keyFile,
		caFile,
		commonName,
	)
	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return v1.NewAdapterClient(conn), conn
}
