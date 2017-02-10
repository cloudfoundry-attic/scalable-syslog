package main

import (
	"flag"
	"log"
	"net"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":8082", "The address to bind to")

	log.Print("Starting fake logs provider...")
	defer log.Print("Closing fake logs provider.")

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %s", err)
	}
	s := grpc.NewServer()
	loggregator.RegisterEgressServer(s, new(logServer))

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

type logServer struct{}

func (s *logServer) Sender(r *loggregator.StreamRequest, server loggregator.Egress_SenderServer) error {
	var i int
	for {
		e := buildEnvelope(i%2 == 0, r.GetFilter().GetSourceId())

		if err := server.Send(e); err != nil {
			return err
		}
		i++
	}

	return nil
}

func buildEnvelope(isLog bool, sourceId string) *loggregator.Envelope {
	if isLog {
		return &loggregator.Envelope{
			Timestamp:  time.Now().UnixNano(),
			SourceUuid: sourceId,
			Message: &loggregator.Envelope_Log{
				Log: &loggregator.Log{
					Payload: []byte("Some happy log"),
					Type:    loggregator.Log_OUT,
				},
			},
		}
	}
	return &loggregator.Envelope{
		Timestamp:  time.Now().UnixNano(),
		SourceUuid: sourceId,
		Message: &loggregator.Envelope_Counter{
			Counter: &loggregator.Counter{
				Name: "some-counter-name",
				Value: &loggregator.Counter_Delta{
					Delta: 42,
				},
			},
		},
	}
}
