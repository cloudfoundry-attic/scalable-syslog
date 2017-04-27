package ingress

import (
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	v2 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
)

type ConnectionBuilder interface {
	Connect() (io.Closer, v2.EgressClient, error)
}

type connection struct {
	closer io.Closer
	client v2.EgressClient
}

// ClientManager manages loggregator egress clients and connections.
type ClientManager struct {
	connector     ConnectionBuilder
	connectionTTL time.Duration
	connections   []*connection
	nextIdx       uint64
	retryWait     time.Duration
	mu            sync.RWMutex
}

type ClientManagerOpts func(*ClientManager)

func WithRetryWait(d time.Duration) func(*ClientManager) {
	return func(c *ClientManager) {
		c.retryWait = d
	}
}

// NewClientManager returns a ClientManager after opening the specified number
// of connections.
func NewClientManager(connector ConnectionBuilder, connCount int, ttl time.Duration, opts ...ClientManagerOpts) *ClientManager {
	c := &ClientManager{
		connector:     connector,
		connectionTTL: ttl,
		connections:   make([]*connection, connCount),
		retryWait:     2 * time.Second,
	}

	for _, opt := range opts {
		opt(c)
	}

	for i := 0; i < connCount; i++ {
		c.openNewConnection(i)
	}

	go c.monitorConnectionsForRolling()

	return c
}

// Next returns the next available loggregator egress client. Next will block
// until a healthy client is available.
func (c *ClientManager) Next() v2.EgressClient {
	for {
		idx := atomic.AddUint64(&c.nextIdx, 1)
		actualIdx := int(idx % uint64(len(c.connections)))

		c.mu.RLock()
		conn := c.connections[actualIdx]
		if conn != nil && conn.client != nil {
			return conn.client
		}
		c.mu.RUnlock()

		c.openNewConnection(actualIdx)
		time.Sleep(c.retryWait)
	}
}

func (c *ClientManager) monitorConnectionsForRolling() {
	for range time.Tick(c.connectionTTL) {
		for i := 0; i < len(c.connections); i++ {
			c.mu.Lock()
			conn := c.connections[i]
			if conn != nil && conn.closer != nil {
				conn.closer.Close()
			}
			c.mu.Unlock()

			c.openNewConnection(i)
		}
	}
}

func (c *ClientManager) openNewConnection(idx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
