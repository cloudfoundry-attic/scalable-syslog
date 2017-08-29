package ingress

import (
	"crypto/tls"
	"io"
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// Connector connects to loggregator egress API
type Connector struct {
	tlsConf  *tls.Config
	balancer *Balancer
}

type LogsProviderClient interface {
	Receiver(ctx context.Context, in *loggregator_v2.EgressRequest, opts ...grpc.CallOption) (loggregator_v2.Egress_ReceiverClient, error)
	BatchedReceiver(ctx context.Context, in *loggregator_v2.EgressBatchRequest, opts ...grpc.CallOption) (loggregator_v2.Egress_BatchedReceiverClient, error)
}

// NewConnector returns a new Connector
func NewConnector(balancer *Balancer, t *tls.Config) *Connector {
	return &Connector{
		balancer: balancer,
		tlsConf:  t,
	}
}

// Connect connects to a loggregator egress API
func (c *Connector) Connect() (io.Closer, LogsProviderClient, error) {
	hp, err := c.balancer.NextHostPort()
	if err != nil {
		return nil, nil, err
	}

	conn, err := grpc.Dial(
		hp,
		grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConf)),
	)

	if err != nil {
		return nil, nil, err
	}

	client := loggregator_v2.NewEgressClient(conn)
	log.Println("Created new connection to loggregator egress API")

	return conn, client, nil
}
