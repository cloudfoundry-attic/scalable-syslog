package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
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

	egressServer, err := newTestEgressServer(*certFile, *keyFile, *caFile, *delay,
		withCN(*commonName),
		withAddr(*addr),
	)
	if err != nil {
		log.Fatalf("failed to build egress server: %s", err)
	}
	egressServer.start()

	var wait chan struct{}
	<-wait
}

func buildEnvelope(isLog bool, sourceId string, id int) *loggregator_v2.Envelope {
	if isLog {
		return &loggregator_v2.Envelope{
			DeprecatedTags: map[string]*loggregator_v2.Value{
				"source_type":     {Data: &loggregator_v2.Value_Text{"APP"}},
				"source_instance": {Data: &loggregator_v2.Value_Text{"3"}},
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
		DeprecatedTags: map[string]*loggregator_v2.Value{
			"source_type":     {Data: &loggregator_v2.Value_Text{"APP"}},
			"source_instance": {Data: &loggregator_v2.Value_Text{"3"}},
		},
		Timestamp: time.Now().UnixNano(),
		SourceId:  sourceId,
		Message: &loggregator_v2.Envelope_Counter{
			Counter: &loggregator_v2.Counter{
				Name:  "some-counter-name",
				Delta: 42,
			},
		},
	}
}

func buildEnvelopes(isLog bool, sourceId string, id int) []*loggregator_v2.Envelope {
	var envelopes []*loggregator_v2.Envelope
	for i := 0; i < 100; i++ {
		envelopes = append(envelopes, buildEnvelope(isLog, sourceId, id))
	}

	return envelopes
}

type testEgressServer struct {
	addr_      string
	cn         string
	delay      time.Duration
	tlsConfig  *tls.Config
	grpcServer *grpc.Server
	grpc.Stream
}

type egressServerOption func(*testEgressServer)

func withCN(cn string) egressServerOption {
	return func(s *testEgressServer) {
		s.cn = cn
	}
}

func withAddr(addr string) egressServerOption {
	return func(s *testEgressServer) {
		s.addr_ = addr
	}
}

func newTestEgressServer(serverCert, serverKey, caCert string, delay time.Duration, opts ...egressServerOption) (*testEgressServer, error) {
	s := &testEgressServer{
		addr_: "localhost:0",
		delay: delay,
	}

	for _, o := range opts {
		o(s)
	}

	cert, err := tls.LoadX509KeyPair(serverCert, serverKey)
	if err != nil {
		return nil, err
	}

	s.tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ClientAuth:         tls.RequestClientCert,
		InsecureSkipVerify: false,
		ServerName:         s.cn,
	}
	caCertBytes, err := ioutil.ReadFile(caCert)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertBytes)
	s.tlsConfig.RootCAs = caCertPool

	return s, nil
}

func (t *testEgressServer) addr() string {
	return t.addr_
}

func (t *testEgressServer) Receiver(*loggregator_v2.EgressRequest, loggregator_v2.Egress_ReceiverServer) error {
	panic("not implemented")

	return nil
}

func (t *testEgressServer) BatchedReceiver(r *loggregator_v2.EgressBatchRequest, server loggregator_v2.Egress_BatchedReceiverServer) error {
	var i int
	for {
		e := buildEnvelopes(i%2 == 0, r.GetLegacySelector().GetSourceId(), i)

		log.Printf("sending envelope: %d", i)
		if err := server.Send(&loggregator_v2.EnvelopeBatch{Batch: e}); err != nil {
			return err
		}
		log.Printf("sent envelope: %d", i)
		i++
		time.Sleep(t.delay)
	}
}

func (t *testEgressServer) start() error {
	listener, err := net.Listen("tcp4", t.addr_)
	if err != nil {
		return err
	}
	t.addr_ = listener.Addr().String()

	var opts []grpc.ServerOption
	if t.tlsConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(t.tlsConfig)))
	}
	t.grpcServer = grpc.NewServer(opts...)

	loggregator_v2.RegisterEgressServer(t.grpcServer, t)

	go t.grpcServer.Serve(listener)

	return nil
}

func (t *testEgressServer) stop() {
	t.grpcServer.Stop()
}
