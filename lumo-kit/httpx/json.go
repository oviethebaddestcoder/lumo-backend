package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/trace"
)

// MaxBody is the default request body limit (1 MiB).
const MaxBody = 1 << 20

// Decode reads a JSON body strictly: unknown fields and trailing data are errors.
func Decode(w http.ResponseWriter, r *http.Request, v any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		return errs.New(http.StatusUnsupportedMediaType, "unsupported_media_type", "Send JSON.")
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		switch {
		case errors.As(err, &mbe):
			return errs.New(http.StatusRequestEntityTooLarge, "body_too_large", "Request body is too large.")
		case errors.Is(err, io.EOF):
			return errs.BadRequest("empty_body", "Request body is empty.")
		default:
			return errs.BadRequest("invalid_json", fmt.Sprintf("Invalid JSON: %v", err))
		}
	}
	if dec.More() {
		return errs.BadRequest("invalid_json", "Only one JSON object is allowed.")
	}
	return nil
}

// JSON writes v with the given status.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// Error writes err as a JSON error. Unknown errors become a generic 500 and
// are logged with the trace ID the client can quote to support.
func Error(w http.ResponseWriter, r *http.Request, err error) {
	if e, ok := errs.As(err); ok {
		if e.Status >= 500 {
			slog.ErrorContext(r.Context(), "request failed", "err", err, "code", e.Code)
		}
		body := map[string]any{"error": e}
		JSON(w, e.Status, body)
		return
	}
	slog.ErrorContext(r.Context(), "unhandled error", "err", err, "path", r.URL.Path)
	JSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{
		"code": "internal", "message": "Something went wrong. Please try again.",
		"trace_id": trace.TraceID(r.Context()),
	}})
}

// Handler is an http.HandlerFunc that returns an error.
type Handler func(w http.ResponseWriter, r *http.Request) error

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h(w, r); err != nil {
		Error(w, r, err)
	}
}

// Page is a cursor-paginated list response.
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}
