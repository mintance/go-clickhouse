package clickhouse

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var errorPattern = regexp.MustCompile(`(?s)Code:\s(\d+)[.,]?(.*)`)

type DbError struct {
	code int
	msg  string
	resp string
}

func (e *DbError) Code() int {
	return e.code
}

func (e *DbError) Message() string {
	return e.msg
}

func (e *DbError) Response() string {
	return e.resp
}

func (e *DbError) Error() string {
	return fmt.Sprintf("clickhouse error: [%d] %s", e.code, e.msg)
}

func (e *DbError) String() string {
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

	// Extract message from e.displayText() if present
	if idx := strings.Index(rest, "e.displayText() = "); idx >= 0 {
		msg = rest[idx+len("e.displayText() = "):]
		// Remove "e.what() = ..." suffix
		if whatIdx := strings.Index(msg, ", e.what()"); whatIdx >= 0 {
			msg = msg[:whatIdx]
		}
	}

	msg = strings.TrimSpace(msg)
	return &DbError{code, msg, resp}
}
