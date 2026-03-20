package clickhouse

import (
	"context"
	"math/rand"
	"sync"
)

// PingErrorFunc is called when a cluster health check fails for a connection.
type PingErrorFunc func(*Conn)

// Cluster manages multiple ClickHouse connections for load balancing.
// Use Check or CheckCtx to update the list of active connections,
// and ActiveConn to get a random healthy connection.
type Cluster struct {
	conn []*Conn
	fail PingErrorFunc

	mu     sync.RWMutex
	active []*Conn
}

// NewCluster creates a cluster from the given connections.
func NewCluster(conn ...*Conn) *Cluster {
	return &Cluster{
		conn: conn,
	}
}

// IsDown returns true if no active connections are available.
func (c *Cluster) IsDown() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.active) == 0
}

// OnCheckError sets a callback invoked when a connection fails its health check.
func (c *Cluster) OnCheckError(f PingErrorFunc) {
	c.fail = f
}

// ActiveConn returns a random active connection, or nil if the cluster is down.
func (c *Cluster) ActiveConn() *Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := len(c.active)
	if n == 0 {
		return nil
	}
	return c.active[rand.Intn(n)]
}

// Check pings all connections and updates the active set.
func (c *Cluster) Check() {
	c.CheckCtx(context.Background())
}

// CheckCtx pings all connections with the given context and updates the active set.
func (c *Cluster) CheckCtx(ctx context.Context) {
	var active []*Conn

	for _, conn := range c.conn {
		if err := conn.Ping(ctx); err == nil {
			active = append(active, conn)
		} else if c.fail != nil {
			c.fail(conn)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.active = active
}
