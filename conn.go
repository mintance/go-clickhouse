package clickhouse

import (
	"context"
	"fmt"
	"strings"
)

const successTestResponse = "Ok."

// Conn represents a connection to a ClickHouse server.
type Conn struct {
	Host     string
	User     string
	Password string
	Database string

	transport Transport
}

// Ping checks if the ClickHouse server is reachable.
func (c *Conn) Ping(ctx context.Context) error {
	res, err := c.transport.Exec(ctx, c, Query{Stmt: ""}, true)
	if err != nil {
		return err
	}
	if !strings.Contains(res, successTestResponse) {
		return fmt.Errorf("clickhouse: unexpected ping response %q, want %q", res, successTestResponse)
	}
	return nil
}
