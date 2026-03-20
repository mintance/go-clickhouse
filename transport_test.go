package clickhouse

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type TestHandler struct {
	Result string
}

func (h *TestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "text/tab-separated-values; charset=UTF-8")
	fmt.Fprint(w, h.Result)
}

func TestExec(t *testing.T) {
	handler := &TestHandler{Result: "1  2.5 clickid68235\n2 -0.14   clickidsdkjhj44"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := &HttpTransport{}
	conn := &Conn{Host: server.URL + "/", transport: transport}
	q := NewQuery("SELECT * FROM testdata")
	resp, err := transport.Exec(context.Background(), conn, q, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, handler.Result, resp)
}

func TestExecReadOnly(t *testing.T) {
	handler := &TestHandler{Result: "1  2.5 clickid68235\n2 -0.14   clickidsdkjhj44"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := &HttpTransport{}
	conn := &Conn{Host: server.URL + "/", transport: transport}
	q := NewQuery(url.QueryEscape("SELECT * FROM testdata"))
	resp, err := transport.Exec(context.Background(), conn, q, true)
	assert.Equal(t, nil, err)
	assert.Equal(t, handler.Result, resp)
}

func TestPrepareHttp(t *testing.T) {
	p := prepareHttp("SELECT * FROM table WHERE key = ?", []interface{}{"test"})
	assert.Equal(t, "SELECT * FROM table WHERE key = 'test'", p)
}

func TestPrepareHttpArray(t *testing.T) {
	p := prepareHttp("INSERT INTO table (arr) VALUES (?)", Row{Array{"val1", "val2"}})
	assert.Equal(t, "INSERT INTO table (arr) VALUES (['val1','val2'])", p)
}

func TestPrepareExecPostRequest(t *testing.T) {
	ctx := context.Background()
	conn := &Conn{Host: "http://127.0.0.0:8123/"}
	q := NewQuery("SELECT * FROM testdata")
	req, err := prepareExecPostRequest(ctx, conn, q)
	assert.Equal(t, nil, err)
	data, err := io.ReadAll(req.Body)
	assert.Equal(t, nil, err)
	assert.Equal(t, "SELECT * FROM testdata", string(data))
}

func TestPrepareExecPostRequestWithExternalData(t *testing.T) {
	ctx := context.Background()
	conn := &Conn{Host: "http://127.0.0.0:8123/"}
	q := NewQuery("SELECT * FROM testdata")
	q.AddExternal("data1", "ID String, Num UInt32", []byte("Hello\t22\nHi\t44"))
	q.AddExternal("extdata", "Num UInt32, Name String", []byte("1	first\n2	second"))

	req, err := prepareExecPostRequest(ctx, conn, q)
	assert.Equal(t, nil, err)
	assert.Equal(t, "SELECT * FROM testdata", req.URL.Query().Get("query"))
	assert.Equal(t, "ID String, Num UInt32", req.URL.Query().Get("data1_structure"))
	assert.Equal(t, "Num UInt32, Name String", req.URL.Query().Get("extdata_structure"))

	mediaType, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	assert.Equal(t, nil, err)
	assert.Equal(t, true, strings.HasPrefix(mediaType, "multipart/"))

	reader := multipart.NewReader(req.Body, params["boundary"])

	p, err := reader.NextPart()
	assert.Equal(t, nil, err)

	data, err := io.ReadAll(p)
	assert.Equal(t, nil, err)
	assert.Equal(t, "Hello\t22\nHi\t44", string(data))

	p, err = reader.NextPart()
	assert.Equal(t, nil, err)

	data, err = io.ReadAll(p)
	assert.Equal(t, nil, err)
	assert.Equal(t, "1\tfirst\n2\tsecond", string(data))
}

type AuthTestHandler struct {
	user     string
	password string
	database string
}

func (h *AuthTestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.user = r.Header.Get("X-ClickHouse-User")
	h.password = r.Header.Get("X-ClickHouse-Key")
	h.database = r.Header.Get("X-ClickHouse-Database")
	fmt.Fprint(w, "Ok.")
}

func TestExecWithAuth(t *testing.T) {
	handler := &AuthTestHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConnWithAuth(server.URL, transport, "admin", "secret")
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Equal(t, "admin", handler.user)
	assert.Equal(t, "secret", handler.password)
}

func TestExecWithAuthReadOnly(t *testing.T) {
	handler := &AuthTestHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConnWithAuth(server.URL, transport, "reader", "pass")
	q := NewQuery(url.QueryEscape("SELECT 1"))
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "reader", handler.user)
	assert.Equal(t, "pass", handler.password)
}

func TestExecWithoutAuth(t *testing.T) {
	handler := &AuthTestHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Equal(t, "", handler.user)
	assert.Equal(t, "", handler.password)
}

func TestExecWithDatabase(t *testing.T) {
	handler := &AuthTestHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConnWithOptions(ConnOptions{
		Host:     server.URL,
		Database: "mydb",
	}, transport)
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Equal(t, "mydb", handler.database)
}

type QueryParamHandler struct {
	queryID   string
	sessionID string
	database  string
	settings  map[string]string
}

func (h *QueryParamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	handler := &QueryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	q.QueryID = "test-query-123"
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "test-query-123", handler.queryID)
}

func TestExecWithSessionID(t *testing.T) {
	handler := &QueryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	q.SessionID = "session-abc"
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "session-abc", handler.sessionID)
}

func TestExecWithSettings(t *testing.T) {
	handler := &QueryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	q.SetSetting("max_rows_to_read", "1000000")
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "1000000", handler.settings["max_rows_to_read"])
}

func TestExecWithDatabaseParam(t *testing.T) {
	handler := &QueryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConnWithOptions(ConnOptions{
		Host:     server.URL,
		Database: "testdb",
	}, transport)
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "testdb", handler.database)
}

type ErrorHandler struct {
	statusCode int
	body       string
}

func (h *ErrorHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(h.statusCode)
	fmt.Fprint(w, h.body)
}

func TestExecHTTPError(t *testing.T) {
	handler := &ErrorHandler{statusCode: 500, body: "Code: 62, e.displayText() = DB::Exception: Syntax error"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT bad query")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.Error(t, err)
	dbErr, ok := err.(*DbError)
	assert.True(t, ok)
	assert.Equal(t, 62, dbErr.Code())
}

func TestExecHTTPErrorGeneric(t *testing.T) {
	handler := &ErrorHandler{statusCode: 403, body: "Forbidden"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 403")
}

type GzipHandler struct {
	Result string
}

func (h *GzipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Accept-Encoding") == "gzip" {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		fmt.Fprint(gz, h.Result)
	} else {
		fmt.Fprint(w, h.Result)
	}
}

func TestExecWithCompression(t *testing.T) {
	handler := &GzipHandler{Result: "1\t2\t3\n4\t5\t6"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := &HttpTransport{Compression: true}
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1, 2, 3")
	resp, err := transport.Exec(context.Background(), conn, q, true)
	assert.NoError(t, err)
	assert.Equal(t, "1\t2\t3\n4\t5\t6", resp)
}

func TestExecContextCancellation(t *testing.T) {
	handler := &TestHandler{Result: "Ok."}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("SELECT 1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := transport.Exec(ctx, conn, q, true)
	assert.Error(t, err)
}

func TestExecPostWithQueryID(t *testing.T) {
	handler := &QueryParamHandler{}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := NewConn(server.URL, transport)
	q := NewQuery("INSERT INTO t VALUES (1)")
	q.QueryID = "insert-123"
	_, err := transport.Exec(context.Background(), conn, q, false)
	assert.NoError(t, err)
	assert.Equal(t, "insert-123", handler.queryID)
}

func BenchmarkPrepareHttp(b *testing.B) {
	params := strings.Repeat("(?,?,?,?,?,?,?,?)", 1000)
	args := make([]interface{}, 8000)
	for i := 0; i < 8000; i++ {
		args[i] = "test"
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		prepareHttp("INSERT INTO t VALUES "+params, args)
	}
}
