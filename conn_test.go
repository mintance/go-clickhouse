package clickhouse

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConnect(t *testing.T) {
	var conn *Conn
	tr := getMockTransport("Ok.")

	conn = NewConn("host.local", tr)
	assert.Equal(t, "http://host.local/", conn.Host)

	conn = NewConn("http://host.local/", tr)
	assert.Equal(t, "http://host.local/", conn.Host)

	conn = NewConn("https://host.local/", tr)
	assert.Equal(t, "https://host.local/", conn.Host)

	conn = NewConn("http:/host.local", tr)
	assert.Equal(t, "http://http:/host.local/", conn.Host)
}

func TestConn_Ping(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("Ok.")
	conn := NewConn("host.local", tr)
	assert.NoError(t, conn.Ping(ctx))
}

func TestConn_Ping2(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("")
	conn := NewConn("host.local", tr)
	assert.Error(t, conn.Ping(ctx))
}

func TestConn_Ping3(t *testing.T) {
	ctx := context.Background()
	tr := badTransport{err: errors.New("Connection timeout")}
	conn := NewConn("host.local", tr)
	assert.Error(t, conn.Ping(ctx))
	assert.Equal(t, "Connection timeout", conn.Ping(ctx).Error())
}

func TestNewConnWithAuth(t *testing.T) {
	tr := getMockTransport("Ok.")
	conn := NewConnWithAuth("host.local", tr, "admin", "secret")
	assert.Equal(t, "http://host.local/", conn.Host)
	assert.Equal(t, "admin", conn.User)
	assert.Equal(t, "secret", conn.Password)
}

func TestNewConnWithAuthEmpty(t *testing.T) {
	tr := getMockTransport("Ok.")
	conn := NewConn("host.local", tr)
	assert.Equal(t, "", conn.User)
	assert.Equal(t, "", conn.Password)
}

func TestNewConnWithOptions(t *testing.T) {
	tr := getMockTransport("Ok.")
	conn := NewConnWithOptions(ConnOptions{
		Host:     "host.local",
		User:     "admin",
		Password: "secret",
		Database: "mydb",
	}, tr)
	assert.Equal(t, "http://host.local/", conn.Host)
	assert.Equal(t, "admin", conn.User)
	assert.Equal(t, "secret", conn.Password)
	assert.Equal(t, "mydb", conn.Database)
}

func TestConn_Database(t *testing.T) {
	tr := getMockTransport("Ok.")
	conn := NewConnWithOptions(ConnOptions{
		Host:     "host.local",
		Database: "testdb",
	}, tr)
	assert.Equal(t, "testdb", conn.Database)
}
