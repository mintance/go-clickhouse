package clickhouse

import (
	"context"
	"errors"
	"strings"
)

// External represents external data that can be sent alongside a query.
// See https://clickhouse.com/docs/en/engines/table-engines/special/external-data
type External struct {
	Name      string
	Structure string
	Data      []byte
}

// Func represents a ClickHouse function call used in query parameters.
type Func struct {
	Name string
	Args any
}

// Query represents a ClickHouse query with optional parameters and settings.
type Query struct {
	Stmt      string
	QueryID   string
	SessionID string
	Settings  map[string]string

	args      []any
	externals []External
}

// AddExternal attaches external data to the query.
func (q *Query) AddExternal(name, structure string, data []byte) {
	q.externals = append(q.externals, External{Name: name, Structure: structure, Data: data})
}

// SetSetting sets a ClickHouse server setting for this query.
// Settings are passed as URL parameters (e.g., max_rows_to_read, max_execution_time).
func (q *Query) SetSetting(key, value string) {
	if q.Settings == nil {
		q.Settings = make(map[string]string)
	}
	q.Settings[key] = value
}

// Iter executes the query and returns an iterator for scanning rows.
func (q Query) Iter(ctx context.Context, conn *Conn) *Iter {
	if conn == nil {
		return &Iter{err: errors.New("connection pointer is nil")}
	}
	resp, err := conn.transport.Exec(ctx, conn, q, false)
	if err != nil {
		return &Iter{err: err}
	}
	if err = errorFromResponse(resp); err != nil {
		return &Iter{err: err}
	}
	return &Iter{text: resp}
}

// Exec executes the query without returning rows.
func (q Query) Exec(ctx context.Context, conn *Conn) error {
	if conn == nil {
		return errors.New("connection pointer is nil")
	}
	resp, err := conn.transport.Exec(ctx, conn, q, false)
	if err != nil {
		return err
	}
	return errorFromResponse(resp)
}

// Iter is a row iterator for ClickHouse query results in TabSeparated format.
type Iter struct {
	err  error
	text string
}

// Error returns the first error encountered during iteration.
func (it *Iter) Error() error {
	return it.err
}

// Scan reads the next row into the provided pointers.
// Returns false when no more rows are available or an error occurs.
func (it *Iter) Scan(vars ...any) bool {
	row := it.fetchNext()
	if len(row) == 0 {
		return false
	}
	cols := strings.Split(row, "\t")
	if len(cols) < len(vars) {
		return false
	}
	for i, v := range vars {
		if err := unmarshal(v, cols[i]); err != nil {
			it.err = err
			return false
		}
	}
	return true
}

func (it *Iter) fetchNext() string {
	pos := strings.Index(it.text, "\n")
	if pos == -1 {
		res := it.text
		it.text = ""
		return res
	}
	res := it.text[:pos]
	it.text = it.text[pos+1:]
	return res
}
