package ingress

import (
	"io"
	"time"

	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
)

type ConnectionBuilder interface {
	Connect() (io.Closer, v2.Egress_ReceiverClient)
}

type connection struct {
	closer io.Closer
	client v2.Egress_ReceiverClient
}

type Consumer struct {
	connector     ConnectionBuilder
	connectionTTL time.Duration
	connections   []connection
}

func NewConsumer(connector ConnectionBuilder, count int, ttl time.Duration) *Consumer {
	c := &Consumer{
		connector:     connector,
		connectionTTL: ttl,
	}

	for i := 0; i < count; i++ {
		closer, client := c.connector.Connect()
		c.connections = append(c.connections, connection{
			closer: closer,
			client: client,
		})
	}

	go c.monitorConnectionsForRolling()

	return c
}

func (c *Consumer) monitorConnectionsForRolling() {
	ticker := time.NewTicker(c.connectionTTL)

	for range ticker.C {
		for i, conn := range c.connections {
			conn.closer.Close()

			closer, client := c.connector.Connect()
			c.connections[i] = connection{
				closer: closer,
				client: client,
			}
		}
	}
}
