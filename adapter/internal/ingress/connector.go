package ingress

import (
	"crypto/tls"
	"io"
	"log"

	"golang.org/x/net/context"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// Connector connects to loggregator egress API
type Connector struct {
	tlsConf  *tls.Config
	balancer *Balancer
}

type LogsProviderClient interface {
	Receiver(ctx context.Context, in *loggregator_v2.EgressRequest) (loggregator_v2.Egress_ReceiverClient, error)
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

	client, closer, err := loggregator.NewEgressClient(hp, c.tlsConf)
	if err != nil {
		return nil, nil, err
	}

	log.Println("Created new connection to loggregator egress API")

	return closer, client, nil
}
