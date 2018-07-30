// fake_adapter: a program to connect and test RLPs.
//
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	addr := flag.String("addr", ":8082", "The address of adapter to make requests at")
	caFile := flag.String("ca", "", "The file path to the CA file")
	certFile := flag.String("cert", "", "The server TLS cert")
	keyFile := flag.String("key", "", "The server TLS private key")
	commonName := flag.String("cn", "", "The server common name for TLS")

	flag.Parse()

	tlsConfig, err := api.NewMutualTLSConfig(*certFile, *keyFile, *caFile, *commonName)

	conn, err := grpc.Dial(
		*addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
	)
	if err != nil {
		log.Fatalf("Error dialing gRPC: %s", err)
	}

	c := loggregator_v2.NewEgressClient(conn)
	if err != nil {
		log.Fatalf("did not create client: %s", err)
	}

	defer conn.Close()
	req := &loggregator_v2.EgressBatchRequest{
		ShardId: "some-shard-id",
	}
	stream, err := c.BatchedReceiver(context.Background(), req)
	if err != nil {
		log.Fatalf("did not establish stream: %s", err)
	}

	for {
		batch, err := stream.Recv()
		if err != nil {
			log.Fatalf("error reading from stream: %s", err)
		}
		fmt.Printf("%#v\n", batch)
	}
}
