package ingress

import (
	"io"
	"log"
	"time"

	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
)

type ConnectionBuilder interface {
	Connect() (io.Closer, v2.Egress_ReceiverClient, error)
}

type connection struct {
	closer io.Closer
	client v2.Egress_ReceiverClient
}

type Consumer struct {
	connector     ConnectionBuilder
	connectionTTL time.Duration
	connections   []*connection
}

func NewConsumer(connector ConnectionBuilder, count int, ttl time.Duration) *Consumer {
	c := &Consumer{
		connector:     connector,
		connectionTTL: ttl,
		connections:   make([]*connection, count),
	}

	for i := 0; i < count; i++ {
		c.openNewConnection(i)
	}

	go c.monitorConnectionsForRolling()

	return c
}

func (c *Consumer) monitorConnectionsForRolling() {
	for range time.Tick(c.connectionTTL) {
		for i, conn := range c.connections {
			if conn != nil {
				conn.closer.Close()
			}

			c.openNewConnection(i)
		}
	}
}

func (c *Consumer) openNewConnection(idx int) {
	closer, client, err := c.connector.Connect()
	if err != nil {
		log.Printf("Failed to connect to loggregator API: %s", err)

		c.connections[idx] = nil
		return
	}

	c.connections[idx] = &connection{
		closer: closer,
		client: client,
	}
}
