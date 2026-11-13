// Package gauth gets OAuth2 access tokens for a Google service account using
// the JWT-bearer grant (RFC 7523). Used for the Google Play Developer API
// (purchase verification) and Firebase Cloud Messaging (push).
package gauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ServiceAccount is the subset of a Google service-account JSON key we need.
type ServiceAccount struct {
	ClientEmail  string `json:"client_email"`
	PrivateKey   string `json:"private_key"`
	PrivateKeyID string `json:"private_key_id"`
	TokenURI     string `json:"token_uri"`
	ProjectID    string `json:"project_id"`
}

// TokenSource caches an access token until shortly before it expires.
type TokenSource struct {
	sa     ServiceAccount
	scopes []string
	client *http.Client

	mu      sync.Mutex
	token   string
	expires time.Time
}

func New(saJSON []byte, client *http.Client, scopes ...string) (*TokenSource, error) {
	var sa ServiceAccount
	if err := json.Unmarshal(saJSON, &sa); err != nil {
		return nil, fmt.Errorf("gauth: parse service account: %w", err)
	}
	if sa.ClientEmail == "" || sa.PrivateKey == "" {
		return nil, errors.New("gauth: service account missing client_email or private_key")
	}
	if sa.TokenURI == "" {
		sa.TokenURI = "https://oauth2.googleapis.com/token"
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &TokenSource{sa: sa, scopes: scopes, client: client}, nil
}

func (t *TokenSource) ProjectID() string { return t.sa.ProjectID }

// Token returns a valid access token.
func (t *TokenSource) Token(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.token != "" && time.Until(t.expires) > time.Minute {
		return t.token, nil
	}
	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(t.sa.PrivateKey))
	if err != nil {
		return "", fmt.Errorf("gauth: parse key: %w", err)
	}
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":   t.sa.ClientEmail,
		"scope": strings.Join(t.scopes, " "),
		"aud":   t.sa.TokenURI,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if t.sa.PrivateKeyID != "" {
		tok.Header["kid"] = t.sa.PrivateKeyID
	}
	assertion, err := tok.SignedString(key)
	if err != nil {
		return "", err
	}
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {assertion}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, t.sa.TokenURI, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gauth: token request: %w", err)
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		Error       string `json:"error"`
		Desc        string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("gauth: decode token: %w", err)
	}
	if resp.StatusCode != http.StatusOK || out.AccessToken == "" {
		return "", fmt.Errorf("gauth: token status %d: %s %s", resp.StatusCode, out.Error, out.Desc)
	}
	t.token = out.AccessToken
	t.expires = now.Add(time.Duration(out.ExpiresIn) * time.Second)
	return t.token, nil
}
