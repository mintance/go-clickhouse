package clickhouse

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testHandler struct {
	result string
}

func (h *testHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "text/tab-separated-values; charset=UTF-8")
	fmt.Fprint(w, h.result)
}

func TestExec(t *testing.T) {
	handler := &testHandler{result: "1  2.5 clickid68235\n2 -0.14   clickidsdkjhj44"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := &HTTPTransport{}
	conn := &Conn{Host: server.URL + "/", transport: transport}
	q := NewQuery("SELECT * FROM testdata")
	resp, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Equal(t, handler.result, resp)
}

func TestExecReadOnly(t *testing.T) {
	handler := &testHandler{result: "1  2.5 clickid68235\n2 -0.14   clickidsdkjhj44"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := &HTTPTransport{}
	conn := &Conn{Host: server.URL + "/", transport: transport}
	q := NewQuery(url.QueryEscape("SELECT * FROM testdata"))
	resp, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, handler.result, resp)
}

func TestPrepareHTTP(t *testing.T) {
	p := prepareHTTP("SELECT * FROM table WHERE key = ?", []any{"test"})
	assert.Equal(t, "SELECT * FROM table WHERE key = 'test'", p)
}

func TestPrepareHTTPArray(t *testing.T) {
	p := prepareHTTP("INSERT INTO table (arr) VALUES (?)", Row{Array{"val1", "val2"}})
	assert.Equal(t, "INSERT INTO table (arr) VALUES (['val1','val2'])", p)
}

func TestPrepareHTTPNoArgs(t *testing.T) {
	p := prepareHTTP("SELECT 1", nil)
	assert.Equal(t, "SELECT 1", p)
}

func TestBuildPostRequest(t *testing.T) {
	ctx := context.Background()
	conn := &Conn{Host: "http://127.0.0.0:8123/"}
	q := NewQuery("SELECT * FROM testdata")
	req, err := buildPostRequest(ctx, conn, q)
	require.NoError(t, err)
	data, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM testdata", string(data))
}

func TestBuildPostRequestWithExternalData(t *testing.T) {
	ctx := context.Background()
	conn := &Conn{Host: "http://127.0.0.0:8123/"}
	q := NewQuery("SELECT * FROM testdata")
	q.AddExternal("data1", "ID String, Num UInt32", []byte("Hello\t22\nHi\t44"))
	q.AddExternal("extdata", "Num UInt32, Name String", []byte("1\tfirst\n2\tsecond"))

	req, err := buildPostRequest(ctx, conn, q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM testdata", req.URL.Query().Get("query"))
	assert.Equal(t, "ID String, Num UInt32", req.URL.Query().Get("data1_structure"))
	assert.Equal(t, "Num UInt32, Name String", req.URL.Query().Get("extdata_structure"))

	mediaType, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(mediaType, "multipart/"))

	reader := multipart.NewReader(req.Body, params["boundary"])

	p, err := reader.NextPart()
	require.NoError(t, err)
	data, err := io.ReadAll(p)
	require.NoError(t, err)
	assert.Equal(t, "Hello\t22\nHi\t44", string(data))

	p, err = reader.NextPart()
	require.NoError(t, err)
	data, err = io.ReadAll(p)
	require.NoError(t, err)
	assert.Equal(t, "1\tfirst\n2\tsecond", string(data))
}

type authTestHandler struct {
	user     string
	password string
	database string
}

func (h *authTestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.user = r.Header.Get("X-ClickHouse-User")
	h.password = r.Header.Get("X-ClickHouse-Key")
	h.database = r.Header.Get("X-ClickHouse-Database")
	fmt.Fprint(w, "Ok.")
}

func TestExecWithAuth(t *testing.T) {
	handler := &authTestHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConnWithAuth(server.URL, transport, "admin", "secret")
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Equal(t, "admin", handler.user)
	assert.Equal(t, "secret", handler.password)
}

func TestExecWithAuthReadOnly(t *testing.T) {
	handler := &authTestHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConnWithAuth(server.URL, transport, "reader", "pass")
	q := NewQuery(url.QueryEscape("SELECT 1"))
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "reader", handler.user)
	assert.Equal(t, "pass", handler.password)
}

func TestExecWithoutAuth(t *testing.T) {
	handler := &authTestHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Empty(t, handler.user)
	assert.Empty(t, handler.password)
}

func TestExecWithDatabase(t *testing.T) {
	handler := &authTestHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConnWithOptions(ConnOptions{Host: server.URL, Database: "mydb"}, transport)
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Equal(t, "mydb", handler.database)
}

type queryParamHandler struct {
	queryID   string
	sessionID string
	database  string
	settings  map[string]string
}

func (h *queryParamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.queryID = r.URL.Query().Get("query_id")
	h.sessionID = r.URL.Query().Get("session_id")
	h.database = r.URL.Query().Get("database")
	h.settings = make(map[string]string)
	for k, v := range r.URL.Query() {
		if k != "query_id" && k != "session_id" && k != "database" && k != "query" {
			h.settings[k] = v[0]
		}
	}
	fmt.Fprint(w, "Ok.")
}

func TestExecWithQueryID(t *testing.T) {
	handler := &queryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	q.QueryID = "test-query-123"
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "test-query-123", handler.queryID)
}

func TestExecWithSessionID(t *testing.T) {
	handler := &queryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	q.SessionID = "session-abc"
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "session-abc", handler.sessionID)
}

func TestExecWithSettings(t *testing.T) {
	handler := &queryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	q.SetSetting("max_rows_to_read", "1000000")
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "1000000", handler.settings["max_rows_to_read"])
}

func TestExecWithDatabaseParam(t *testing.T) {
	handler := &queryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConnWithOptions(ConnOptions{Host: server.URL, Database: "testdb"}, transport)
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "testdb", handler.database)
}

type errorHandler struct {
	statusCode int
	body       string
}

func (h *errorHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(h.statusCode)
	fmt.Fprint(w, h.body)
}

func TestExecHTTPError(t *testing.T) {
	handler := &errorHandler{statusCode: 500, body: "Code: 62, e.displayText() = DB::Exception: Syntax error"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT bad query")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.Error(t, err)

	var dbErr *DBError
	assert.ErrorAs(t, err, &dbErr)
	assert.Equal(t, 62, dbErr.Code())
}

func TestExecHTTPErrorGeneric(t *testing.T) {
	handler := &errorHandler{statusCode: 403, body: "Forbidden"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 403")
}

type gzipHandler struct {
	result string
}

func (h *gzipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Accept-Encoding") == "gzip" {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		fmt.Fprint(gz, h.result)
	} else {
		fmt.Fprint(w, h.result)
	}
}

func TestExecWithCompression(t *testing.T) {
	handler := &gzipHandler{result: "1\t2\t3\n4\t5\t6"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := &HTTPTransport{Compression: true}
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1, 2, 3")
	resp, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "1\t2\t3\n4\t5\t6", resp)
}

func TestExecContextCancellation(t *testing.T) {
	handler := &testHandler{result: "Ok."}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := transport.Exec(ctx, conn, q, true)
	assert.Error(t, err)
}

func TestExecPostWithQueryID(t *testing.T) {
	handler := &queryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHTTPTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("INSERT INTO t VALUES (1)")
	q.QueryID = "insert-123"
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Equal(t, "insert-123", handler.queryID)
}

func BenchmarkPrepareHTTP(b *testing.B) {
	params := strings.Repeat("(?,?,?,?,?,?,?,?)", 1000)
	args := make([]any, 8000)
	for i := range args {
		args[i] = "test"
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		prepareHTTP("INSERT INTO t VALUES "+params, args)
	}
}
