package ingress

import (
	"context"
	"io"

	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

	"google.golang.org/grpc"
)

type Connector struct {
	addr string
	opts []grpc.DialOption
}

func NewConnector(addr string, opts ...grpc.DialOption) *Connector {
	return &Connector{
		addr: addr,
		opts: opts,
	}
}

func (c *Connector) Connect() (io.Closer, v2.Egress_ReceiverClient) {
	conn, err := grpc.Dial(c.addr, c.opts...)
	if err != nil {
		panic(err) // TODO: I SHOULD NOT PANIC
	}

	client := v2.NewEgressClient(conn)
	receiver, err := client.Receiver(context.Background(), new(v2.EgressRequest))
	if err != nil {
		panic(err) // TODO: I SHOULD NOT PANIC
	}

	return conn, receiver
}
