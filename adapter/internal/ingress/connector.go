package ingress

import (
	"crypto/tls"
	"io"
	"log"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var (
	keepAliveParams = keepalive.ClientParameters{
		Time:                15 * time.Second,
		Timeout:             15 * time.Second,
		PermitWithoutStream: true,
	}
)

// Connector connects to loggregator egress API
type Connector struct {
	balancers   []Balancer
	dialTimeout time.Duration
	tlsConf     *tls.Config
}

// LogsProviderClient describes the gRPC interface for communicating with
// Loggregator.
type LogsProviderClient interface {
	Receiver(
		ctx context.Context,
		in *loggregator_v2.EgressRequest,
		opts ...grpc.CallOption,
	) (loggregator_v2.Egress_ReceiverClient, error)

	BatchedReceiver(
		ctx context.Context,
		in *loggregator_v2.EgressBatchRequest,
		opts ...grpc.CallOption,
	) (loggregator_v2.Egress_BatchedReceiverClient, error)
}

// Balancer cycles through a collection of host ports. It will return an error
// when there is no host port available.
type Balancer interface {
	NextHostPort() (string, error)
}

// NewConnector returns a new Connector
func NewConnector(b []Balancer, dt time.Duration, t *tls.Config) *Connector {
	return &Connector{
		balancers:   b,
		dialTimeout: dt,
		tlsConf:     t,
	}
}

// Connect connects to a loggregator egress API
func (c *Connector) Connect() (io.Closer, LogsProviderClient, error) {
	var err error
	for _, balancer := range c.balancers {
		var hostPort string
		hostPort, err = balancer.NextHostPort()
		if err != nil {
			continue
		}

		var conn *grpc.ClientConn
		conn, err = grpc.Dial(
			hostPort,
			grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConf)),
			grpc.WithKeepaliveParams(keepAliveParams),
			grpc.WithBlock(),
			grpc.WithTimeout(c.dialTimeout),
		)
		if err != nil {
			continue
		}

		client := loggregator_v2.NewEgressClient(conn)
		log.Println("Created new connection to loggregator egress API")

		return conn, client, nil
	}

	return nil, nil, err
}
