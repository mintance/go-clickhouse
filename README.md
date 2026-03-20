# go-clickhouse

[![Tests](https://github.com/mintance/go-clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/mintance/go-clickhouse/actions/workflows/test.yml) [![Go Report](https://goreportcard.com/badge/github.com/mintance/go-clickhouse)](https://goreportcard.com/report/github.com/mintance/go-clickhouse) ![](https://img.shields.io/github/license/mintance/go-clickhouse.svg)

Lightweight Go client for [ClickHouse](https://clickhouse.com/) using the HTTP interface.

## Features

- HTTP interface (GET for read-only, POST for mutations)
- `context.Context` support for timeouts and cancellation
- Connection pooling (reused `http.Client`)
- Gzip compression
- Authentication (`X-ClickHouse-User` / `X-ClickHouse-Key`)
- Database selection
- Query ID and Session ID support
- Per-query settings
- External data for query processing
- Cluster mode with health checks
- Type support: int, uint, float, string, bool, time.Time, arrays, Nullable

## Install

```
go get github.com/mintance/go-clickhouse
```

Requires Go 1.21+.

## Examples

### Query rows

```go
ctx := context.Background()
conn := clickhouse.NewConn("localhost:8123", clickhouse.NewHTTPTransport())
query := clickhouse.NewQuery("SELECT name, date FROM clicks")
iter := query.Iter(ctx, conn)
var (
    name string
    date string
)
for iter.Scan(&name, &date) {
    //
}
if iter.Error() != nil {
    log.Panicln(iter.Error())
}
```

### Insert

```go
ctx := context.Background()
conn := clickhouse.NewConn("localhost:8123", clickhouse.NewHTTPTransport())
query, err := clickhouse.BuildInsert("clicks",
    clickhouse.Columns{"name", "date", "sourceip"},
    clickhouse.Row{"Test name", "2016-01-01 21:01:01", clickhouse.Func{"IPv4StringToNum", "192.0.2.192"}},
)
if err == nil {
    err = query.Exec(ctx, conn)
}
```

### Multi insert

```go
query, err := clickhouse.BuildMultiInsert("clicks",
    clickhouse.Columns{"name", "count"},
    clickhouse.Rows{
        clickhouse.Row{"first", 1},
        clickhouse.Row{"second", 2},
    },
)
if err == nil {
    err = query.Exec(ctx, conn)
}
```

### Authentication and database

```go
transport := clickhouse.NewHTTPTransport()
conn := clickhouse.NewConnWithAuth("localhost:8123", transport, "user", "password")
```

Or with all options:

```go
conn := clickhouse.NewConnWithOptions(clickhouse.ConnOptions{
    Host:     "localhost:8123",
    User:     "user",
    Password: "password",
    Database: "mydb",
}, transport)
```

### Query ID and Session ID

```go
query := clickhouse.NewQuery("SELECT 1")
query.QueryID = "my-query-123"   // track in system.query_log
query.SessionID = "my-session"   // persist SET commands across queries
```

### Per-query settings

```go
query := clickhouse.NewQuery("SELECT number FROM system.numbers LIMIT 100")
query.SetSetting("max_rows_to_read", "1000000")
query.SetSetting("max_execution_time", "60")
```

### External data

[See ClickHouse documentation](https://clickhouse.com/docs/en/engines/table-engines/special/external-data)

```go
query := clickhouse.NewQuery("SELECT Num, Name FROM extdata")
query.AddExternal("extdata", "Num UInt32, Name String", []byte("1\tfirst\n2\tsecond"))

iter := query.Iter(ctx, conn)
var (
    num  int
    name string
)
for iter.Scan(&num, &name) {
    //
}
```

### Compression

```go
transport := &clickhouse.HTTPTransport{
    Timeout:     5 * time.Second,
    Compression: true,
}
conn := clickhouse.NewConn("localhost:8123", transport)
```

### Context with timeout

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

iter := query.Iter(ctx, conn)
```

## Cluster

Cluster is useful if you have several servers with the same `Distributed` table. Requests are sent to a random active connection to balance load.

- `cluster.Check()` pings all connections and filters active ones
- `cluster.CheckCtx(ctx)` same with context support
- `cluster.ActiveConn()` returns a random active connection
- `cluster.OnCheckError()` is called when any connection fails

**Important**: Call `Check()` at least once after initialization. We recommend calling it continuously so `ActiveConn()` always returns an active connection.

```go
transport := clickhouse.NewHTTPTransport()
conn1 := clickhouse.NewConn("host1", transport)
conn2 := clickhouse.NewConn("host2", transport)

cluster := clickhouse.NewCluster(conn1, conn2)
cluster.OnCheckError(func(c *clickhouse.Conn) {
    log.Printf("ClickHouse connection failed %s", c.Host)
})
// Ping connections every second
go func() {
    for {
        cluster.Check()
        time.Sleep(time.Second)
    }
}()
```

## Transport options

### Timeout

```go
transport := clickhouse.NewHTTPTransport()
transport.Timeout = 5 * time.Second

conn := clickhouse.NewConn("localhost:8123", transport)
```
