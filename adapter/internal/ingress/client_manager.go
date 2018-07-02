package ingress

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type ConnectionBuilder interface {
	Connect() (io.Closer, LogsProviderClient, error)
}

type connection struct {
	closer    io.Closer
	client    LogsProviderClient
	createdAt time.Time
}

// ClientManager manages loggregator egress clients and connections.
type ClientManager struct {
	checkInterval time.Duration
	connectionTTL time.Duration
	connector     ConnectionBuilder
	nextIdx       uint64
	retryWait     time.Duration

	mu          sync.RWMutex
	connections []*connection
}

type ClientManagerOpts func(*ClientManager)

func WithRetryWait(d time.Duration) func(*ClientManager) {
	return func(c *ClientManager) {
		c.retryWait = d
	}
}

// NewClientManager returns a ClientManager after opening the specified number
// of connections.
func NewClientManager(
	connector ConnectionBuilder,
	connCount int,
	ttl time.Duration,
	check time.Duration,
	opts ...ClientManagerOpts,
) *ClientManager {
	c := &ClientManager{
		connector:     connector,
		connectionTTL: ttl,
		connections:   make([]*connection, connCount),
		retryWait:     2 * time.Second,
		checkInterval: check,
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
func (c *ClientManager) Next() LogsProviderClient {
	for {
		idx := atomic.AddUint64(&c.nextIdx, 1)
		actualIdx := int(idx % uint64(len(c.connections)))

		c.mu.RLock()
		conn := c.connections[actualIdx]
		c.mu.RUnlock()

		if conn != nil && conn.client != nil {
			return conn.client
		}

		time.Sleep(c.retryWait)
	}
}

func (c *ClientManager) monitorConnectionsForRolling() {
	for range time.Tick(c.checkInterval) {
		for i := 0; i < len(c.connections); i++ {
			c.mu.RLock()
			conn := c.connections[i]
			c.mu.RUnlock()

			if conn == nil {
				c.openNewConnection(i)
				continue
			}

			if !conn.client.Valid() || time.Since(conn.createdAt) >= c.connectionTTL {
				conn.closer.Close()
				c.openNewConnection(i)
			}
		}
	}
}

func (c *ClientManager) openNewConnection(idx int) {
	closer, client, err := c.connector.Connect()
	if err != nil {
		c.mu.Lock()
		c.connections[idx] = nil
		c.mu.Unlock()

		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.connections[idx] = &connection{
		closer:    closer,
		client:    client,
		createdAt: time.Now(),
	}
}
