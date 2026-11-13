// Package errs defines application errors that map cleanly to HTTP responses.
// Handlers return *errs.Error for expected failures; anything else is a 500
// whose details are logged but never shown to the client.
package errs

import (
	"errors"
	"fmt"
	"net/http"
)

type Error struct {
	Status  int            `json:"-"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
	cause   error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return e.Code + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.cause }

// Wrap attaches an underlying cause (logged, not returned to clients).
func (e *Error) Wrap(err error) *Error {
	c := *e
	c.cause = err
	return &c
}

// With adds a field (e.g. retry_after) returned to the client.
func (e *Error) With(k string, v any) *Error {
	c := *e
	c.Fields = map[string]any{}
	for kk, vv := range e.Fields {
		c.Fields[kk] = vv
	}
	c.Fields[k] = v
	return &c
}

func New(status int, code, msg string) *Error {
	return &Error{Status: status, Code: code, Message: msg}
}

func BadRequest(code, msg string) *Error   { return New(http.StatusBadRequest, code, msg) }
func Unauthorized(code, msg string) *Error { return New(http.StatusUnauthorized, code, msg) }
func Forbidden(code, msg string) *Error    { return New(http.StatusForbidden, code, msg) }
func NotFound(code, msg string) *Error     { return New(http.StatusNotFound, code, msg) }
func Conflict(code, msg string) *Error     { return New(http.StatusConflict, code, msg) }
func TooMany(code, msg string) *Error      { return New(http.StatusTooManyRequests, code, msg) }
func Unavailable(code, msg string) *Error  { return New(http.StatusServiceUnavailable, code, msg) }

var (
	ErrUnauthenticated = Unauthorized("unauthenticated", "Sign in to continue.")
	ErrForbidden       = Forbidden("forbidden", "You can't do that.")
	ErrNotFound        = NotFound("not_found", "Not found.")
	ErrAdultOnly       = Forbidden("adult_only", "You must be 18 or older for this.")
	ErrRateLimited     = TooMany("rate_limited", "Too many requests. Try again shortly.")
)

// As extracts an *Error from err.
func As(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}
