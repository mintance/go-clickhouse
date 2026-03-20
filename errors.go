package clickhouse

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var errorPattern = regexp.MustCompile(`(?s)Code:\s(\d+)[.,]?(.*)`)

// DBError represents a ClickHouse server error with a numeric code.
type DBError struct {
	code int
	msg  string
	resp string
}

// Code returns the ClickHouse error code.
func (e *DBError) Code() int {
	return e.code
}

// Message returns the parsed error message.
func (e *DBError) Message() string {
	return e.msg
}

// Response returns the full server response that produced this error.
func (e *DBError) Response() string {
	return e.resp
}

// Error implements the error interface.
func (e *DBError) Error() string {
	return fmt.Sprintf("clickhouse error: [%d] %s", e.code, e.msg)
}

// String returns a debug representation of the error.
func (e *DBError) String() string {
	return fmt.Sprintf("[error code=%d message=%q]", e.code, e.msg)
}

func errorFromResponse(resp string) error {
	if resp == "" {
		return nil
	}
	if !errorPattern.MatchString(resp) {
		return nil
	}

	matches := errorPattern.FindStringSubmatch(resp)
	code, err := strconv.Atoi(matches[1])
	if err != nil {
		return err
	}

	var msg string
	rest := matches[2]

	// Extract message from e.displayText() if present.
	if idx := strings.Index(rest, "e.displayText() = "); idx >= 0 {
		msg = rest[idx+len("e.displayText() = "):]
		// Remove "e.what() = ..." suffix.
		if whatIdx := strings.Index(msg, ", e.what()"); whatIdx >= 0 {
			msg = msg[:whatIdx]
		}
	}

	msg = strings.TrimSpace(msg)
	return &DBError{code, msg, resp}
}
