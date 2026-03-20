package clickhouse

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const httpTransportBodyType = "text/plain"

// Transport is the interface for executing ClickHouse queries over a network protocol.
type Transport interface {
	Exec(ctx context.Context, conn *Conn, q Query, readOnly bool) (string, error)
}

// HTTPTransport executes queries over the ClickHouse HTTP interface.
// A single HTTPTransport reuses connections across requests.
type HTTPTransport struct {
	Timeout     time.Duration
	Compression bool

	once   sync.Once
	client *http.Client
}

// Compile-time check that HTTPTransport implements Transport.
var _ Transport = (*HTTPTransport)(nil)

func (t *HTTPTransport) getClient() *http.Client {
	t.once.Do(func() {
		t.client = &http.Client{
			Timeout: t.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	})
	return t.client
}

// Exec sends a query to ClickHouse and returns the response body.
// When readOnly is true, the query is sent as a GET request.
func (t *HTTPTransport) Exec(ctx context.Context, conn *Conn, q Query, readOnly bool) (string, error) {
	var req *http.Request
	var err error

	query := prepareHTTP(q.Stmt, q.args)

	if readOnly {
		req, err = t.buildGetRequest(ctx, conn, q, query)
	} else {
		req, err = buildPostRequest(ctx, conn, q)
	}
	if err != nil {
		return "", err
	}

	t.setHeaders(conn, req)

	resp, err := t.getClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := t.readBody(resp)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		if dbErr := errorFromResponse(body); dbErr != nil {
			return "", dbErr
		}
		return "", fmt.Errorf("clickhouse: HTTP %d: %s", resp.StatusCode, body)
	}

	return body, nil
}

func (t *HTTPTransport) buildGetRequest(ctx context.Context, conn *Conn, q Query, query string) (*http.Request, error) {
	params := url.Values{}
	if len(query) > 0 {
		params.Set("query", query)
	}
	addConnParams(conn, params)
	addQueryParams(q, params)

	reqURL := conn.Host
	if encoded := params.Encode(); len(encoded) > 0 {
		reqURL += "?" + encoded
	}
	return http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
}

func (t *HTTPTransport) readBody(resp *http.Response) (string, error) {
	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return "", err
		}
		defer gz.Close()
		reader = gz
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(reader); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (t *HTTPTransport) setHeaders(conn *Conn, req *http.Request) {
	if conn.User != "" {
		req.Header.Set("X-ClickHouse-User", conn.User)
	}
	if conn.Password != "" {
		req.Header.Set("X-ClickHouse-Key", conn.Password)
	}
	if conn.Database != "" {
		req.Header.Set("X-ClickHouse-Database", conn.Database)
	}
	if t.Compression {
		req.Header.Set("Accept-Encoding", "gzip")
	}
}

func addConnParams(conn *Conn, params url.Values) {
	if conn.Database != "" {
		params.Set("database", conn.Database)
	}
}

func addQueryParams(q Query, params url.Values) {
	if q.QueryID != "" {
		params.Set("query_id", q.QueryID)
	}
	if q.SessionID != "" {
		params.Set("session_id", q.SessionID)
	}
	for k, v := range q.Settings {
		params.Set(k, v)
	}
}

func buildPostRequest(ctx context.Context, conn *Conn, q Query) (*http.Request, error) {
	query := prepareHTTP(q.Stmt, q.args)

	if len(q.externals) > 0 {
		return buildMultipartPostRequest(ctx, conn, q, query)
	}
	return buildSimplePostRequest(ctx, conn, q, query)
}

func buildMultipartPostRequest(ctx context.Context, conn *Conn, q Query, query string) (*http.Request, error) {
	params := url.Values{}
	if len(query) > 0 {
		params.Set("query", query)
	}
	for _, ext := range q.externals {
		params.Set(ext.Name+"_structure", ext.Structure)
	}
	addConnParams(conn, params)
	addQueryParams(q, params)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	for _, ext := range q.externals {
		part, err := writer.CreateFormFile(ext.Name, ext.Name)
		if err != nil {
			return nil, err
		}
		if _, err = part.Write(ext.Data); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	reqURL := conn.Host + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req, nil
}

func buildSimplePostRequest(ctx context.Context, conn *Conn, q Query, query string) (*http.Request, error) {
	params := url.Values{}
	addConnParams(conn, params)
	addQueryParams(q, params)

	postURL := conn.Host
	if encoded := params.Encode(); len(encoded) > 0 {
		postURL += "?" + encoded
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", httpTransportBodyType)
	return req, nil
}

func prepareHTTP(stmt string, args []any) string {
	if len(args) == 0 {
		return stmt
	}

	var res []byte
	k := 0
	for _, ch := range []byte(stmt) {
		if ch == '?' {
			res = append(res, []byte(marshal(args[k]))...)
			k++
		} else {
			res = append(res, ch)
		}
	}
	return string(res)
}
