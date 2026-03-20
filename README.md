# go-clickhouse

[![Tests](https://github.com/mintance/go-clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/mintance/go-clickhouse/actions/workflows/test.yml) [![Go Report](https://goreportcard.com/badge/github.com/mintance/go-clickhouse)](https://goreportcard.com/report/github.com/mintance/go-clickhouse) [![GoDoc](https://pkg.go.dev/badge/github.com/mintance/go-clickhouse)](https://pkg.go.dev/github.com/mintance/go-clickhouse) ![](https://img.shields.io/github/license/mintance/go-clickhouse.svg)

Lightweight Go client for [ClickHouse](https://clickhouse.com/) using the HTTP interface.

## Features

- HTTP interface (GET for read-only, POST for mutations)
- `context.Context` support for timeouts and cancellation
- Connection pooling (reused `http.Client`)
- Gzip compression (request `Accept-Encoding`, response decompression)
- Authentication via `X-ClickHouse-User` / `X-ClickHouse-Key` headers
- Database selection via header and URL parameter
- Query ID and Session ID support
- Per-query ClickHouse settings
- External data for query processing
- Cluster mode with ping-based health checks and load balancing
- HTTP status code checking with structured error responses
- Type support: `int8`–`int64`, `uint8`–`uint64`, `float32`, `float64`, `string`, `bool`, `time.Time` (Date and DateTime), arrays, Nullable (`*string`, `*int64`, `*float64`)

## Install

```
go get github.com/mintance/go-clickhouse
```

Requires Go 1.21+.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/mintance/go-clickhouse"
)

func main() {
    ctx := context.Background()
    conn := clickhouse.NewConn("localhost:8123", clickhouse.NewHTTPTransport())

    if err := conn.Ping(ctx); err != nil {
        log.Fatal(err)
    }

    query := clickhouse.NewQuery("SELECT number, toString(number) FROM system.numbers LIMIT 5")
    iter := query.Iter(ctx, conn)

    var num int
    var str string
    for iter.Scan(&num, &str) {
        fmt.Println(num, str)
    }
    if err := iter.Error(); err != nil {
        log.Fatal(err)
    }
}
```

## Examples

### Query rows

```go
ctx := context.Background()
conn := clickhouse.NewConn("localhost:8123", clickhouse.NewHTTPTransport())
query := clickhouse.NewQuery("SELECT name, date FROM clicks WHERE id = ?", 42)
iter := query.Iter(ctx, conn)

var name, date string
for iter.Scan(&name, &date) {
    fmt.Println(name, date)
}
if err := iter.Error(); err != nil {
    log.Fatal(err)
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
if err != nil {
    log.Fatal(err)
}
if err := query.Exec(ctx, conn); err != nil {
    log.Fatal(err)
}
```

### Multi insert

```go
ctx := context.Background()
query, err := clickhouse.BuildMultiInsert("clicks",
    clickhouse.Columns{"name", "count"},
    clickhouse.Rows{
        clickhouse.Row{"first", 1},
        clickhouse.Row{"second", 2},
    },
)
if err != nil {
    log.Fatal(err)
}
if err := query.Exec(ctx, conn); err != nil {
    log.Fatal(err)
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
// Track query in system.query_log
query := clickhouse.NewQuery("SELECT 1")
query.QueryID = "my-query-123"
iter := query.Iter(ctx, conn)

// Use sessions to persist SET commands across queries
set := clickhouse.NewQuery("SET max_threads = 1")
set.SessionID = "my-session"
set.Exec(ctx, conn)

get := clickhouse.NewQuery("SELECT getSetting('max_threads')")
get.SessionID = "my-session"
iter = get.Iter(ctx, conn)
```

### Per-query settings

Pass any [ClickHouse server setting](https://clickhouse.com/docs/en/operations/settings/settings) as a URL parameter:

```go
query := clickhouse.NewQuery("SELECT number FROM system.numbers LIMIT 100")
query.SetSetting("max_rows_to_read", "1000000")
query.SetSetting("max_execution_time", "60")
```

### External data

[See ClickHouse documentation](https://clickhouse.com/docs/en/engines/table-engines/special/external-data)

```go
ctx := context.Background()
query := clickhouse.NewQuery("SELECT Num, Name FROM extdata")
query.AddExternal("extdata", "Num UInt32, Name String", []byte("1\tfirst\n2\tsecond"))

iter := query.Iter(ctx, conn)
var num int
var name string
for iter.Scan(&num, &name) {
    fmt.Println(num, name)
}
```

### Compression

```go
transport := &clickhouse.HTTPTransport{
    Timeout:     5 * time.Second,
    Compression: true, // sends Accept-Encoding: gzip, decompresses responses
}
conn := clickhouse.NewConn("localhost:8123", transport)
```

### Context with timeout

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

iter := query.Iter(ctx, conn)
```

### Nullable types

ClickHouse `Nullable` columns return `\N` for NULL values. Use pointer types to handle them:

```go
var name *string   // Nullable(String)
var count *int64   // Nullable(Int64)
var rate *float64  // Nullable(Float64)
iter.Scan(&name, &count, &rate)
// name == nil means NULL
```

### Error handling

```go
err := query.Exec(ctx, conn)
if err != nil {
    var dbErr *clickhouse.DBError
    if errors.As(err, &dbErr) {
        fmt.Printf("ClickHouse error %d: %s\n", dbErr.Code(), dbErr.Message())
        fmt.Println("Full response:", dbErr.Response())
    } else {
        // Transport error, HTTP error, etc.
        fmt.Println("Error:", err)
    }
}
```

Non-200 HTTP responses are also returned as errors. If the response body contains a ClickHouse error code, it is parsed into a `*DBError`. Otherwise, you get a generic error like `"clickhouse: HTTP 403: Forbidden"`.

## Cluster

Cluster manages multiple connections for load balancing across servers with the same `Distributed` table.

- `cluster.Check()` — pings all connections and updates the active set
- `cluster.CheckCtx(ctx)` — same with context support
- `cluster.ActiveConn()` — returns a random active connection
- `cluster.IsDown()` — returns true if no connections are active
- `cluster.OnCheckError(func)` — callback when a connection fails its health check

**Important**: Call `Check()` at least once after initialization. We recommend calling it continuously so `ActiveConn()` always returns a healthy connection.

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

// Use in requests
conn := cluster.ActiveConn()
if conn != nil {
    query.Exec(ctx, conn)
}
```

## Transport options

### Timeout

```go
transport := clickhouse.NewHTTPTransport()
transport.Timeout = 5 * time.Second

conn := clickhouse.NewConn("localhost:8123", transport)
```

### Connection pooling

`HTTPTransport` reuses a single `http.Client` with connection pooling (100 max idle connections, 10 per host, 90s idle timeout). No configuration needed — just reuse the same transport instance.

## Supported types

| ClickHouse Type | Go Type | Marshal | Unmarshal |
|---|---|---|---|
| Int8–Int64 | `int8`–`int64` | Yes | Yes |
| UInt8–UInt64 | `uint8`–`uint64` | Yes | Yes |
| Float32, Float64 | `float32`, `float64` | Yes | Yes |
| String | `string` | Yes | Yes |
| Bool (UInt8) | `bool` | Yes | Yes |
| Date | `time.Time` | Yes | Yes |
| DateTime | `time.Time` | Yes | Yes |
| Array(T) | `[]int`, `[]string`, `Array` | Yes | Yes |
| Nullable(String) | `*string` | — | Yes |
| Nullable(Int64) | `*int64` | — | Yes |
| Nullable(Float64) | `*float64` | — | Yes |
