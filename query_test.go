package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockTransport struct {
	response string
}

type badTransport struct {
	response string
	err      error
}

func (m mockTransport) Exec(_ context.Context, _ *Conn, _ Query, _ bool) (string, error) {
	return m.response, nil
}

func (m badTransport) Exec(_ context.Context, _ *Conn, _ Query, _ bool) (string, error) {
	return "", m.err
}

func TestQuery_IterDBError(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("Code: 62, ")
	conn := NewConn(getHost(), tr)
	iter := NewQuery("SELECT 1").Iter(ctx, conn)
	assert.Error(t, iter.Error())
	assert.Equal(t, 62, iter.Error().(*DBError).Code())
}

func TestQuery_IterTransportError(t *testing.T) {
	ctx := context.Background()
	tr := badTransport{err: errors.New("no connection")}
	conn := NewConn(getHost(), tr)
	iter := NewQuery("SELECT 1").Iter(ctx, conn)
	assert.Error(t, iter.Error())
	assert.Equal(t, "no connection", iter.Error().Error())
}

func TestIter_ScanInt(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("1\t2")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 1, 2").Iter(ctx, conn)
	var v1, v2 int
	ok := iter.Scan(&v1, &v2)
	assert.True(t, ok)
	assert.Equal(t, 1, v1)
	assert.Equal(t, 2, v2)
}

func TestIter_ScanInt64(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("1\t2")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 1, 2").Iter(ctx, conn)
	var v1, v2 int64
	ok := iter.Scan(&v1, &v2)
	assert.True(t, ok)
	assert.Equal(t, int64(1), v1)
	assert.Equal(t, int64(2), v2)
}

func TestIter_ScanString(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("test1\ttest2")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 'test1', 'test2'").Iter(ctx, conn)
	var v1, v2 string
	ok := iter.Scan(&v1, &v2)
	assert.True(t, ok)
	assert.Equal(t, "test1", v1)
	assert.Equal(t, "test2", v2)
}

func TestIter_ScanStringMultiple(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("test1\ttest2\ntest3\ttest4")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 'test1', 'test2'").Iter(ctx, conn)
	var v1, v2 string
	assert.True(t, iter.Scan(&v1, &v2))
	assert.Equal(t, "test1", v1)
	assert.Equal(t, "test2", v2)

	assert.True(t, iter.Scan(&v1, &v2))
	assert.Equal(t, "test3", v1)
	assert.Equal(t, "test4", v2)
}

func TestIter_ScanErrors(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("test1\ttest2\ntest3\ttest4")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 'test1', 'test2'").Iter(ctx, conn)
	var v1, v2, v3 string
	assert.False(t, iter.Scan(&v1, &v2, &v3))
	assert.NoError(t, iter.Error())

	var u1 Conn
	assert.False(t, iter.Scan(&u1))
	assert.Error(t, iter.Error())

	tr = getMockTransport("")
	conn = NewConn(getHost(), tr)
	iter = NewQuery("SELECT 'test1', 'test2'").Iter(ctx, conn)
	assert.False(t, iter.Scan(&u1))
	assert.NoError(t, iter.Error())
}

func TestQuery_Exec(t *testing.T) {
	ctx := context.Background()
	tr := getMockTransport("")
	conn := NewConn(getHost(), tr)

	err := NewQuery("INSERT INTO table VALUES 1").Exec(ctx, conn)
	assert.NoError(t, err)

	tr = getMockTransport("Code: 69, ")
	conn = NewConn(getHost(), tr)
	err = NewQuery("INSERT INTO table VALUES 1").Exec(ctx, conn)
	assert.Error(t, err)
	assert.Equal(t, 69, err.(*DBError).Code())
}

func TestQuery_ExecNilConn(t *testing.T) {
	ctx := context.Background()
	err := NewQuery("SELECT 1").Exec(ctx, nil)
	assert.Error(t, err)
}

func TestQuery_IterNilConn(t *testing.T) {
	ctx := context.Background()
	iter := NewQuery("INSERT 1").Iter(ctx, nil)
	assert.Error(t, iter.Error())
}

func TestQuery_WithQueryID(t *testing.T) {
	q := NewQuery("SELECT 1")
	q.QueryID = "test-query-123"
	assert.Equal(t, "test-query-123", q.QueryID)
}

func TestQuery_WithSessionID(t *testing.T) {
	q := NewQuery("SELECT 1")
	q.SessionID = "session-abc"
	assert.Equal(t, "session-abc", q.SessionID)
}

func TestQuery_SetSetting(t *testing.T) {
	q := NewQuery("SELECT 1")
	q.SetSetting("max_rows_to_read", "1000000")
	q.SetSetting("max_execution_time", "60")
	assert.Equal(t, "1000000", q.Settings["max_rows_to_read"])
	assert.Equal(t, "60", q.Settings["max_execution_time"])
}

func getMockTransport(resp string) mockTransport {
	return mockTransport{response: resp}
}

func getHost() string {
	return "host.local"
}
