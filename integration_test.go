//go:build integration

package clickhouse

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getIntegrationConn(t *testing.T) *Conn {
	t.Helper()
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost:8123"
	}
	transport := &HTTPTransport{Timeout: 10 * time.Second}
	conn := NewConn(host, transport)
	return conn
}

func getIntegrationConnWithDB(t *testing.T, db string) *Conn {
	t.Helper()
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost:8123"
	}
	transport := &HTTPTransport{Timeout: 10 * time.Second}
	conn := NewConnWithOptions(ConnOptions{
		Host:     host,
		Database: db,
	}, transport)
	return conn
}

func TestIntegration_Ping(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()
	err := conn.Ping(ctx)
	assert.NoError(t, err)
}

func TestIntegration_PingWithContext(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := conn.Ping(ctx)
	assert.NoError(t, err)
}

func TestIntegration_PingCancelled(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := conn.Ping(ctx)
	assert.Error(t, err)
}

func TestIntegration_Select(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT 1 AS num, 'hello' AS str")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var num int
	var str string
	ok := iter.Scan(&num, &str)
	assert.True(t, ok)
	assert.Equal(t, 1, num)
	assert.Equal(t, "hello", str)
}

func TestIntegration_SelectMultipleRows(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT number, toString(number) FROM system.numbers LIMIT 5")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var rows []int
	var num int
	var str string
	for iter.Scan(&num, &str) {
		rows = append(rows, num)
	}
	assert.NoError(t, iter.Error())
	assert.Equal(t, []int{0, 1, 2, 3, 4}, rows)
}

func TestIntegration_SelectTypes(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT toInt8(42), toInt16(1000), toInt32(100000), toInt64(9999999999), toFloat32(3.14), toFloat64(2.718281828)")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var i8 int8
	var i16 int16
	var i32 int32
	var i64 int64
	var f32 float32
	var f64 float64
	ok := iter.Scan(&i8, &i16, &i32, &i64, &f32, &f64)
	assert.True(t, ok)
	assert.Equal(t, int8(42), i8)
	assert.Equal(t, int16(1000), i16)
	assert.Equal(t, int32(100000), i32)
	assert.Equal(t, int64(9999999999), i64)
	assert.InDelta(t, float32(3.14), f32, 0.01)
	assert.InDelta(t, 2.718281828, f64, 0.000001)
}

func TestIntegration_SelectUintTypes(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT toUInt8(255), toUInt16(65535), toUInt32(4294967295), toUInt64(18446744073709551615)")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var u8 uint8
	var u16 uint16
	var u32 uint32
	var u64 uint64
	ok := iter.Scan(&u8, &u16, &u32, &u64)
	assert.True(t, ok)
	assert.Equal(t, uint8(255), u8)
	assert.Equal(t, uint16(65535), u16)
	assert.Equal(t, uint32(4294967295), u32)
	assert.Equal(t, uint64(18446744073709551615), u64)
}

func TestIntegration_SelectDateTime(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT toDateTime('2023-06-15 10:30:45')")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var dt time.Time
	ok := iter.Scan(&dt)
	assert.True(t, ok)
	assert.Equal(t, 2023, dt.Year())
	assert.Equal(t, time.June, dt.Month())
	assert.Equal(t, 15, dt.Day())
	assert.Equal(t, 10, dt.Hour())
	assert.Equal(t, 30, dt.Minute())
	assert.Equal(t, 45, dt.Second())
}

func TestIntegration_SelectDate(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT toDate('2023-06-15')")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var d time.Time
	ok := iter.Scan(&d)
	assert.True(t, ok)
	assert.Equal(t, time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC), d)
}

func TestIntegration_SelectArray(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT [1, 2, 3]")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var arr []int
	ok := iter.Scan(&arr)
	assert.True(t, ok)
	assert.Equal(t, []int{1, 2, 3}, arr)
}

func TestIntegration_SelectStringArray(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT ['hello', 'world']")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var arr []string
	ok := iter.Scan(&arr)
	assert.True(t, ok)
	assert.Equal(t, []string{"hello", "world"}, arr)
}

func TestIntegration_SelectNullable(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT toNullable(toInt64(42)), toNullable(toInt64(NULL))")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var val *int64
	var null *int64
	ok := iter.Scan(&val, &null)
	assert.True(t, ok)
	require.NotNil(t, val)
	assert.Equal(t, int64(42), *val)
	assert.Nil(t, null)
}

func TestIntegration_SelectBool(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT 1, 0")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var t1, t2 bool
	ok := iter.Scan(&t1, &t2)
	assert.True(t, ok)
	assert.True(t, t1)
	assert.False(t, t2)
}

func TestIntegration_CreateInsertSelect(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	// Create table
	create := NewQuery("CREATE TABLE IF NOT EXISTS test_integration (id UInt32, name String, value Float64) ENGINE = Memory")
	err := create.Exec(ctx, conn)
	require.NoError(t, err)

	// Clean up
	defer func() {
		drop := NewQuery("DROP TABLE IF EXISTS test_integration")
		drop.Exec(ctx, conn)
	}()

	// Insert data using BuildInsert
	q, err := BuildInsert("test_integration", Columns{"id", "name", "value"}, Row{uint32(1), "Alice", 3.14})
	require.NoError(t, err)
	err = q.Exec(ctx, conn)
	require.NoError(t, err)

	// Insert more rows using BuildMultiInsert
	q, err = BuildMultiInsert("test_integration", Columns{"id", "name", "value"}, Rows{
		Row{uint32(2), "Bob", 2.71},
		Row{uint32(3), "Charlie", 1.41},
	})
	require.NoError(t, err)
	err = q.Exec(ctx, conn)
	require.NoError(t, err)

	// Select and verify
	sel := NewQuery("SELECT id, name, value FROM test_integration ORDER BY id")
	iter := sel.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	type row struct {
		id    uint32
		name  string
		value float64
	}
	var results []row
	var r row
	for iter.Scan(&r.id, &r.name, &r.value) {
		results = append(results, r)
	}
	assert.NoError(t, iter.Error())
	require.Len(t, results, 3)
	assert.Equal(t, uint32(1), results[0].id)
	assert.Equal(t, "Alice", results[0].name)
	assert.Equal(t, uint32(2), results[1].id)
	assert.Equal(t, "Bob", results[1].name)
	assert.Equal(t, uint32(3), results[2].id)
	assert.Equal(t, "Charlie", results[2].name)
}

func TestIntegration_ExecError(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT * FROM nonexistent_table_12345")
	err := q.Exec(ctx, conn)
	assert.Error(t, err)

	dbErr, ok := err.(*DBError)
	assert.True(t, ok)
	assert.Greater(t, dbErr.Code(), 0)
}

func TestIntegration_IterError(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("INVALID SQL QUERY")
	iter := q.Iter(ctx, conn)
	assert.Error(t, iter.Error())
}

func TestIntegration_Placeholder(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT ? + ?", 10, 20)
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var result int
	ok := iter.Scan(&result)
	assert.True(t, ok)
	assert.Equal(t, 30, result)
}

func TestIntegration_PlaceholderString(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT ?", "hello world")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var result string
	ok := iter.Scan(&result)
	assert.True(t, ok)
	assert.Equal(t, "hello world", result)
}

func TestIntegration_Database(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	// Create a test database
	create := NewQuery("CREATE DATABASE IF NOT EXISTS test_db_integration")
	err := create.Exec(ctx, conn)
	require.NoError(t, err)

	defer func() {
		drop := NewQuery("DROP DATABASE IF EXISTS test_db_integration")
		drop.Exec(ctx, conn)
	}()

	// Create table in that database
	createTbl := NewQuery("CREATE TABLE IF NOT EXISTS test_db_integration.test_tbl (id UInt32) ENGINE = Memory")
	err = createTbl.Exec(ctx, conn)
	require.NoError(t, err)

	// Connect with database option and query without qualifying table name
	dbConn := getIntegrationConnWithDB(t, "test_db_integration")

	insert := NewQuery("INSERT INTO test_tbl VALUES (?)", uint32(42))
	err = insert.Exec(ctx, dbConn)
	require.NoError(t, err)

	sel := NewQuery("SELECT id FROM test_tbl")
	iter := sel.Iter(ctx, dbConn)
	require.NoError(t, iter.Error())

	var id uint32
	ok := iter.Scan(&id)
	assert.True(t, ok)
	assert.Equal(t, uint32(42), id)
}

func TestIntegration_QueryID(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	queryID := fmt.Sprintf("test-query-%d", time.Now().UnixNano())
	q := NewQuery("SELECT 1")
	q.QueryID = queryID
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var result int
	ok := iter.Scan(&result)
	assert.True(t, ok)
	assert.Equal(t, 1, result)

	// Verify query_id was recorded in system.query_log
	// Need a small delay for the async query log flush
	time.Sleep(500 * time.Millisecond)
	flush := NewQuery("SYSTEM FLUSH LOGS")
	flush.Exec(ctx, conn)

	check := NewQuery("SELECT count() FROM system.query_log WHERE query_id = ?", queryID)
	checkIter := check.Iter(ctx, conn)
	require.NoError(t, checkIter.Error())

	var count int
	ok = checkIter.Scan(&count)
	assert.True(t, ok)
	assert.Greater(t, count, 0)
}

func TestIntegration_Settings(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT number FROM system.numbers LIMIT 100")
	q.SetSetting("max_rows_to_read", "10")

	iter := q.Iter(ctx, conn)
	// Should error because max_rows_to_read=10 but we're trying to read up to 100
	assert.Error(t, iter.Error())
}

func TestIntegration_Compression(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost:8123"
	}
	transport := &HTTPTransport{
		Timeout:     10 * time.Second,
		Compression: true,
	}
	conn := NewConn(host, transport)
	ctx := context.Background()

	// Generate a larger result to benefit from compression
	q := NewQuery("SELECT number, toString(number) FROM system.numbers LIMIT 1000")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	count := 0
	var num int
	var str string
	for iter.Scan(&num, &str) {
		count++
	}
	assert.NoError(t, iter.Error())
	assert.Equal(t, 1000, count)
}

func TestIntegration_ExternalData(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	q := NewQuery("SELECT * FROM ext_data ORDER BY id")
	q.AddExternal("ext_data", "id UInt32, name String", []byte("1\tAlice\n2\tBob\n3\tCharlie"))

	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var results []string
	var id uint32
	var name string
	for iter.Scan(&id, &name) {
		results = append(results, name)
	}
	assert.NoError(t, iter.Error())
	assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, results)
}

func TestIntegration_SessionID(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	sessionID := fmt.Sprintf("test-session-%d", time.Now().UnixNano())

	// Set a variable in session
	set := NewQuery("SET max_threads = 1")
	set.SessionID = sessionID
	err := set.Exec(ctx, conn)
	require.NoError(t, err)

	// Read it back in the same session
	get := NewQuery("SELECT getSetting('max_threads')")
	get.SessionID = sessionID
	iter := get.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var val string
	ok := iter.Scan(&val)
	assert.True(t, ok)
	assert.Equal(t, "1", val)
}

func TestIntegration_Cluster(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost:8123"
	}
	transport := &HTTPTransport{Timeout: 5 * time.Second}

	conn1 := NewConn(host, transport)
	conn2 := NewConn("localhost:19999", transport) // bad port

	cl := NewCluster(conn1, conn2)
	assert.True(t, cl.IsDown())

	cl.Check()

	assert.False(t, cl.IsDown())

	active := cl.ActiveConn()
	require.NotNil(t, active)
	assert.Contains(t, active.Host, host)
}

func TestIntegration_EscapeSpecialChars(t *testing.T) {
	conn := getIntegrationConn(t)
	ctx := context.Background()

	// Test string with special characters
	q := NewQuery("SELECT ?", "hello's \"world\" \\test")
	iter := q.Iter(ctx, conn)
	require.NoError(t, iter.Error())

	var result string
	ok := iter.Scan(&result)
	assert.True(t, ok)
	assert.Equal(t, "hello's \"world\" \\test", result)
}
