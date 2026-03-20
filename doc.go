// Package clickhouse provides a lightweight Go client for ClickHouse
// using the HTTP interface.
//
// It supports query execution, row scanning, batch inserts, external data,
// gzip compression, authentication, database selection, query/session IDs,
// per-query settings, and cluster-aware load balancing.
//
// Basic usage:
//
//	ctx := context.Background()
//	transport := clickhouse.NewHTTPTransport()
//	conn := clickhouse.NewConn("localhost:8123", transport)
//
//	query := clickhouse.NewQuery("SELECT name, age FROM users WHERE id = ?", 42)
//	iter := query.Iter(ctx, conn)
//
//	var name string
//	var age int
//	for iter.Scan(&name, &age) {
//	    fmt.Println(name, age)
//	}
//	if err := iter.Error(); err != nil {
//	    log.Fatal(err)
//	}
package clickhouse
