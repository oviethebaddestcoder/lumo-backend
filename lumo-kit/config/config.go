// Package config reads typed settings from environment variables.
// Missing required values are collected and reported together at startup,
// so a misconfigured pod fails fast with one clear message.
package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env collects lookups and their errors.
type Env struct {
	missing []string
	bad     []string
}

func New() *Env { return &Env{} }

func (e *Env) Str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (e *Env) Required(key string) string {
	v := os.Getenv(key)
	if v == "" {
		e.missing = append(e.missing, key)
	}
	return v
}

func (e *Env) Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.bad = append(e.bad, key+"="+v)
		return def
	}
	return n
}

func (e *Env) Int64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		e.bad = append(e.bad, key+"="+v)
		return def
	}
	return n
}

func (e *Env) Bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.bad = append(e.bad, key+"="+v)
		return def
	}
	return b
}

func (e *Env) Duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.bad = append(e.bad, key+"="+v)
		return def
	}
	return d
}

// List splits a comma-separated value, trimming blanks.
func (e *Env) List(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Base64 decodes a base64 (std or url) value; used for keys and JSON blobs in env.
func (e *Env) Base64(key string, required bool) []byte {
	v := os.Getenv(key)
	if v == "" {
		if required {
			e.missing = append(e.missing, key)
		}
		return nil
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(v); err == nil {
			return b
		}
	}
	e.bad = append(e.bad, key+" (not base64)")
	return nil
}

// Err returns a single error describing every missing or malformed variable.
func (e *Env) Err() error {
	if len(e.missing) == 0 && len(e.bad) == 0 {
		return nil
	}
	var parts []string
	if len(e.missing) > 0 {
		parts = append(parts, "missing: "+strings.Join(e.missing, ", "))
	}
	if len(e.bad) > 0 {
		parts = append(parts, "invalid: "+strings.Join(e.bad, ", "))
	}
	return fmt.Errorf("config: %s", strings.Join(parts, "; "))
}

// DecodeBase64 decodes std or URL base64; returns nil if s is not base64.
func DecodeBase64(s string) []byte {
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil {
			return b
		}
	}
	return nil
}
