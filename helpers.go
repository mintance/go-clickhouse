package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

// Type aliases for query building.
type (
	Column  string
	Columns []string
	Row     []any
	Rows    []Row
	Array   []any
)

// ConnOptions holds all parameters for creating a new connection.
type ConnOptions struct {
	Host     string
	User     string
	Password string
	Database string
}

// NewHTTPTransport creates a new HTTP transport with default settings.
func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{}
}

// NewConn creates a new connection to the given host.
func NewConn(host string, t Transport) *Conn {
	return NewConnWithAuth(host, t, "", "")
}

// NewConnWithAuth creates a new connection with user/password authentication.
func NewConnWithAuth(host string, t Transport, user, password string) *Conn {
	return NewConnWithOptions(ConnOptions{
		Host:     host,
		User:     user,
		Password: password,
	}, t)
}

// NewConnWithOptions creates a new connection from the given options.
func NewConnWithOptions(opts ConnOptions, t Transport) *Conn {
	host := normalizeHost(opts.Host)
	return &Conn{
		Host:      host,
		transport: t,
		User:      opts.User,
		Password:  opts.Password,
		Database:  opts.Database,
	}
}

func normalizeHost(host string) string {
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "http://" + host
	}
	return strings.TrimRight(host, "/") + "/"
}

// NewQuery creates a new query with placeholder arguments.
// Use ? as placeholders: NewQuery("SELECT * FROM t WHERE id = ?", 42).
func NewQuery(stmt string, args ...any) Query {
	return Query{
		Stmt: stmt,
		args: args,
	}
}

// BuildInsert creates an INSERT query for a single row.
func BuildInsert(tbl string, cols Columns, row Row) (Query, error) {
	return BuildMultiInsert(tbl, cols, Rows{row})
}

// BuildMultiInsert creates an INSERT query for multiple rows.
func BuildMultiInsert(tbl string, cols Columns, rows Rows) (Query, error) {
	if len(cols) == 0 || len(rows) == 0 {
		return Query{}, errors.New("rows and cols cannot be empty")
	}

	colCount := len(cols)
	rowCount := len(rows)
	args := make([]any, 0, colCount*rowCount)

	for _, row := range rows {
		if len(row) != colCount {
			return Query{}, errors.New("row item count does not match column count")
		}
		args = append(args, row...)
	}

	binds := "(" + strings.Repeat("?,", colCount)
	binds = binds[:len(binds)-1] + "),"
	batch := strings.Repeat(binds, rowCount)
	batch = batch[:len(batch)-1]

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl, strings.Join(cols, ","), batch)
	return NewQuery(stmt, args...), nil
}
