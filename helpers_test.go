package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewHTTPTransport(t *testing.T) {
	tr := NewHTTPTransport()
	assert.IsType(t, &HTTPTransport{}, tr)
}

func TestNewQuery(t *testing.T) {
	stmt := "SELECT * FROM table WHERE ?"
	q := NewQuery(stmt, 1)
	assert.Equal(t, stmt, q.Stmt)
	assert.Equal(t, []any{1}, q.args)
}

func TestBuildInsert(t *testing.T) {
	q, err := BuildInsert("test", Columns{"col1", "col2"}, Row{"val1", "val2"})
	assert.Equal(t, "INSERT INTO test (col1,col2) VALUES (?,?)", q.Stmt)
	assert.Equal(t, []any{"val1", "val2"}, q.args)
	assert.NoError(t, err)

	q, err = BuildInsert("test", Columns{"col1", "col2"}, Row{"val1"})
	assert.Empty(t, q.Stmt)
	assert.Error(t, err)
}

func TestBuildInsertArray(t *testing.T) {
	q, err := BuildInsert("test", Columns{"col1", "col2"}, Row{"val1", Array{"val2", "val3"}})
	assert.Equal(t, "INSERT INTO test (col1,col2) VALUES (?,?)", q.Stmt)
	assert.Equal(t, []any{"val1", Array{"val2", "val3"}}, q.args)
	assert.NoError(t, err)
}

func TestBuildMultiInsert(t *testing.T) {
	q, err := BuildMultiInsert("test", Columns{"col1", "col2"}, Rows{
		Row{"val1", "val2"},
		Row{"val3", "val4"},
	})
	assert.Equal(t, "INSERT INTO test (col1,col2) VALUES (?,?),(?,?)", q.Stmt)
	assert.Equal(t, []any{"val1", "val2", "val3", "val4"}, q.args)
	assert.NoError(t, err)

	q, err = BuildMultiInsert("test", Columns{"col1", "col2"}, Rows{
		Row{"val1", "val2"},
		Row{"val3"},
	})
	assert.Empty(t, q.Stmt)
	assert.Error(t, err)

	q, err = BuildMultiInsert("test", Columns{}, Rows{})
	assert.Empty(t, q.Stmt)
	assert.Error(t, err)
}

func BenchmarkNewInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		BuildInsert("test", Columns{"col1", "col2"}, Row{"val1", "val2"})
	}
}

func getRows(n int, r Row) Rows {
	res := make(Rows, n)
	for i := range res {
		res[i] = r
	}
	return res
}

func BenchmarkNewMultiInsert100(b *testing.B) {
	columns := Columns{"col1", "col2", "col3", "col4"}
	rows := getRows(100, Row{"val1", "val2", "val3", "val4"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		BuildMultiInsert("test", columns, rows)
	}
}

func BenchmarkNewMultiInsert1000(b *testing.B) {
	columns := Columns{"col1", "col2", "col3", "col4"}
	rows := getRows(1000, Row{"val1", "val2", "val3", "val4"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		BuildMultiInsert("test", columns, rows)
	}
}
