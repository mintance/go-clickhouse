package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	tr := getMockTransport("Ok.")

	tests := []struct {
		name     string
		input    string
		wantHost string
	}{
		{"bare host", "host.local", "http://host.local/"},
		{"http with trailing slash", "http://host.local/", "http://host.local/"},
		{"https with trailing slash", "https://host.local/", "https://host.local/"},
		{"malformed scheme", "http:/host.local", "http://http:/host.local/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := NewConn(tt.input, tr)
			assert.Equal(t, tt.wantHost, conn.Host)
		})
	}
}

func TestConn_PingOK(t *testing.T) {
	ctx := context.Background()
	conn := NewConn("host.local", getMockTransport("Ok."))
	assert.NoError(t, conn.Ping(ctx))
}

func TestConn_PingBadResponse(t *testing.T) {
	ctx := context.Background()
	conn := NewConn("host.local", getMockTransport(""))
	assert.Error(t, conn.Ping(ctx))
}

func TestConn_PingTransportError(t *testing.T) {
	ctx := context.Background()
	tr := badTransport{err: errors.New("connection timeout")}
	conn := NewConn("host.local", tr)
	assert.Error(t, conn.Ping(ctx))
	assert.Equal(t, "connection timeout", conn.Ping(ctx).Error())
}

func TestNewConnWithAuth(t *testing.T) {
	conn := NewConnWithAuth("host.local", getMockTransport("Ok."), "admin", "secret")
	assert.Equal(t, "http://host.local/", conn.Host)
	assert.Equal(t, "admin", conn.User)
	assert.Equal(t, "secret", conn.Password)
}

func TestNewConnWithAuthEmpty(t *testing.T) {
	conn := NewConn("host.local", getMockTransport("Ok."))
	assert.Empty(t, conn.User)
	assert.Empty(t, conn.Password)
}

func TestNewConnWithOptions(t *testing.T) {
	conn := NewConnWithOptions(ConnOptions{
		Host:     "host.local",
		User:     "admin",
		Password: "secret",
		Database: "mydb",
	}, getMockTransport("Ok."))
	assert.Equal(t, "http://host.local/", conn.Host)
	assert.Equal(t, "admin", conn.User)
	assert.Equal(t, "secret", conn.Password)
	assert.Equal(t, "mydb", conn.Database)
}

func TestConn_Database(t *testing.T) {
	conn := NewConnWithOptions(ConnOptions{
		Host:     "host.local",
		Database: "testdb",
	}, getMockTransport("Ok."))
	assert.Equal(t, "testdb", conn.Database)
}
