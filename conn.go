package clickhouse

import (
	"context"
	"fmt"
	"strings"
)

const (
	successTestResponse = "Ok."
)

type Conn struct {
	Host     string
	User     string
	Password string
	Database string

	transport Transport
}

func (c *Conn) Ping(ctx context.Context) (err error) {
	var res string
	res, err = c.transport.Exec(ctx, c, Query{Stmt: ""}, true)
	if err == nil {
		if !strings.Contains(res, successTestResponse) {
			err = fmt.Errorf("Clickhouse host response was '%s', expected '%s'.", res, successTestResponse)
		}
	}

	return err
}
