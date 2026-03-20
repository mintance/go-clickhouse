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

const (
	httpTransportBodyType = "text/plain"
)

type Transport interface {
	Exec(ctx context.Context, conn *Conn, q Query, readOnly bool) (res string, err error)
}

type HttpTransport struct {
	Timeout     time.Duration
	Compression bool

	once   sync.Once
	client *http.Client
}

func (t *HttpTransport) getClient() *http.Client {
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

func (t *HttpTransport) Exec(ctx context.Context, conn *Conn, q Query, readOnly bool) (res string, err error) {
	var req *http.Request
	query := prepareHttp(q.Stmt, q.args)
	client := t.getClient()

	if readOnly {
		params := url.Values{}
		if len(query) > 0 {
			params.Set("query", query)
		}
		t.addConnParams(conn, params)
		t.addQueryParams(q, params)

		reqURL := conn.Host
		if encoded := params.Encode(); len(encoded) > 0 {
			reqURL += "?" + encoded
		}
		req, err = http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if err != nil {
			return "", err
		}
	} else {
		req, err = prepareExecPostRequest(ctx, conn, q)
		if err != nil {
			return "", err
		}
	}

	t.setHeaders(conn, req)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gz, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			return "", gzErr
		}
		defer gz.Close()
		reader = gz
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(reader)
	if err != nil {
		return "", err
	}

	body := buf.String()

	if resp.StatusCode != http.StatusOK {
		if dbErr := errorFromResponse(body); dbErr != nil {
			return "", dbErr
		}
		return "", fmt.Errorf("clickhouse: HTTP %d: %s", resp.StatusCode, body)
	}

	return body, nil
}

func (t *HttpTransport) setHeaders(conn *Conn, req *http.Request) {
	if len(conn.User) > 0 {
		req.Header.Set("X-ClickHouse-User", conn.User)
	}
	if len(conn.Password) > 0 {
		req.Header.Set("X-ClickHouse-Key", conn.Password)
	}
	if len(conn.Database) > 0 {
		req.Header.Set("X-ClickHouse-Database", conn.Database)
	}
	if t.Compression {
		req.Header.Set("Accept-Encoding", "gzip")
	}
}

func (t *HttpTransport) addConnParams(conn *Conn, params url.Values) {
	if len(conn.Database) > 0 {
		params.Set("database", conn.Database)
	}
}

func (t *HttpTransport) addQueryParams(q Query, params url.Values) {
	if len(q.QueryID) > 0 {
		params.Set("query_id", q.QueryID)
	}
	if len(q.SessionID) > 0 {
		params.Set("session_id", q.SessionID)
	}
	for k, v := range q.Settings {
		params.Set(k, v)
	}
}

func prepareExecPostRequest(ctx context.Context, conn *Conn, q Query) (*http.Request, error) {
	query := prepareHttp(q.Stmt, q.args)
	var req *http.Request
	var err error

	if len(q.externals) > 0 {
		params := url.Values{}
		if len(query) > 0 {
			params.Set("query", query)
		}
		for _, ext := range q.externals {
			params.Set(ext.Name+"_structure", ext.Structure)
		}
		if len(q.QueryID) > 0 {
			params.Set("query_id", q.QueryID)
		}
		if len(q.SessionID) > 0 {
			params.Set("session_id", q.SessionID)
		}
		if len(conn.Database) > 0 {
			params.Set("database", conn.Database)
		}
		for k, v := range q.Settings {
			params.Set(k, v)
		}

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for _, ext := range q.externals {
			part, partErr := writer.CreateFormFile(ext.Name, ext.Name)
			if partErr != nil {
				return nil, partErr
			}
			_, partErr = part.Write(ext.Data)
			if partErr != nil {
				return nil, partErr
			}
		}

		err = writer.Close()
		if err != nil {
			return nil, err
		}

		reqURL := conn.Host + "?" + params.Encode()
		req, err = http.NewRequestWithContext(ctx, "POST", reqURL, body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
	} else {
		postURL := conn.Host
		params := url.Values{}
		if len(q.QueryID) > 0 {
			params.Set("query_id", q.QueryID)
		}
		if len(q.SessionID) > 0 {
			params.Set("session_id", q.SessionID)
		}
		if len(conn.Database) > 0 {
			params.Set("database", conn.Database)
		}
		for k, v := range q.Settings {
			params.Set(k, v)
		}
		if encoded := params.Encode(); len(encoded) > 0 {
			postURL += "?" + encoded
		}

		req, err = http.NewRequestWithContext(ctx, "POST", postURL, strings.NewReader(query))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", httpTransportBodyType)
	}
	return req, err
}

func prepareHttp(stmt string, args []interface{}) string {
	var res []byte
	buf := []byte(stmt)
	res = make([]byte, 0)
	k := 0
	for _, ch := range buf {
		if ch == '?' {
			res = append(res, []byte(marshal(args[k]))...)
			k++
		} else {
			res = append(res, ch)
		}
	}

	return string(res)
}
