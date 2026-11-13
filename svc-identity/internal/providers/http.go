package providers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

func getJSON(ctx context.Context, c *http.Client, u, bearer string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("provider request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		// 4xx from the provider means the credential is bad, not that we failed.
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return fmt.Errorf("%w: provider status %d", ErrInvalid, resp.StatusCode)
		}
		return fmt.Errorf("provider status %d", resp.StatusCode)
	}
	return json.Unmarshal(body, v)
}

func urlq(s string) string { return url.QueryEscape(s) }

func hmacHex(key, msg string) string {
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(msg))
	return hex.EncodeToString(m.Sum(nil))
}
