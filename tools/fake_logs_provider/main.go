package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/go-loggregator/testhelpers"
)

func main() {
	addr := flag.String("addr", ":8082", "The address to bind to")
	caFile := flag.String("ca", "", "The file path to the CA file")
	certFile := flag.String("cert", "", "The server TLS cert")
	keyFile := flag.String("key", "", "The server TLS private key")
	commonName := flag.String("cn", "", "The server common name for TLS")
	delay := flag.Duration("delay", time.Second, "The time the server waits between sending messages")

	flag.Parse()

	log.Print("Starting fake logs provider...")
	defer log.Print("Closing fake logs provider.")

	logServer := &logServer{delay: *delay}

	egressServer, err := testhelpers.NewTestEgressServer(*certFile, *keyFile, *caFile,
		testhelpers.WithCN(*commonName),
		testhelpers.WithAddr(*addr),
	)
	if err != nil {
		log.Fatalf("failed to build egress server: %s", err)
	}
	egressServer.Start(logServer.Receiver)
}

type logServer struct {
	delay time.Duration
}

func (s *logServer) Receiver(r *loggregator_v2.EgressRequest, server loggregator_v2.Egress_ReceiverServer) error {
	var i int
	for {
		e := buildEnvelope(i%2 == 0, r.GetFilter().GetSourceId(), i)

		log.Printf("sending envelope: %d", i)
		if err := server.Send(e); err != nil {
			return err
		}
		log.Printf("sent envelope: %d", i)
		i++
		time.Sleep(s.delay)
	}

	return nil
}

func buildEnvelope(isLog bool, sourceId string, id int) *loggregator_v2.Envelope {
	if isLog {
		return &loggregator_v2.Envelope{
			Tags: map[string]*loggregator_v2.Value{
				"source_type":     {&loggregator_v2.Value_Text{"APP"}},
				"source_instance": {&loggregator_v2.Value_Text{"3"}},
			},
			Timestamp: time.Now().UnixNano(),
			SourceId:  sourceId,
			Message: &loggregator_v2.Envelope_Log{
				Log: &loggregator_v2.Log{
					Payload: []byte(fmt.Sprintf("Some happy log: %d", id)),
					Type:    loggregator_v2.Log_OUT,
				},
			},
		}
	}
	return &loggregator_v2.Envelope{
		Tags: map[string]*loggregator_v2.Value{
			"source_type":     {&loggregator_v2.Value_Text{"APP"}},
			"source_instance": {&loggregator_v2.Value_Text{"3"}},
		},
		Timestamp: time.Now().UnixNano(),
		SourceId:  sourceId,
		Message: &loggregator_v2.Envelope_Counter{
			Counter: &loggregator_v2.Counter{
				Name: "some-counter-name",
				Value: &loggregator_v2.Counter_Delta{
					Delta: 42,
				},
			},
		},
	}
}
