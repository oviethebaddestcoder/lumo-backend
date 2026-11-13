// Package auth issues and verifies Lumo access tokens.
//
// Access tokens are short-lived (15 min) EdDSA (Ed25519) JWTs signed by the
// identity service. Every service verifies them locally against identity's
// JWKS, so no service trusts headers forwarded by another hop. Key rotation:
// identity publishes the current and previous public keys; verifiers refresh
// the JWKS when they see an unknown kid.
package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	Issuer   = "lumo-identity"
	Audience = "lumo-api"
)

// Claims carried in every access token.
type Claims struct {
	jwt.RegisteredClaims
	SessionID string   `json:"sid"`
	Adult     bool     `json:"adult"`
	Roles     []string `json:"roles,omitempty"`
}

// Principal is the authenticated caller.
type Principal struct {
	UserID    string
	SessionID string
	Adult     bool
	Roles     []string
	Token     string
}

func (p Principal) HasRole(r string) bool { return slices.Contains(p.Roles, r) }

// ---- signing (identity service only) ----

type SigningKey struct {
	KID     string
	Private ed25519.PrivateKey
}

// ParsePrivateKeyPEM reads a PKCS#8 Ed25519 private key.
// Generate one with: openssl genpkey -algorithm ed25519
func ParsePrivateKeyPEM(b []byte) (ed25519.PrivateKey, error) {
	blk, _ := pem.Decode(b)
	if blk == nil {
		return nil, errors.New("auth: no PEM block")
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("auth: parse key: %w", err)
	}
	pk, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("auth: key is not Ed25519")
	}
	return pk, nil
}

// Signer signs with the first key; all keys are published in the JWKS.
type Signer struct {
	keys []SigningKey
	ttl  time.Duration
}

func NewSigner(ttl time.Duration, keys ...SigningKey) (*Signer, error) {
	if len(keys) == 0 {
		return nil, errors.New("auth: at least one signing key required")
	}
	return &Signer{keys: keys, ttl: ttl}, nil
}

func (s *Signer) TTL() time.Duration { return s.ttl }

// Issue returns a signed access token and its expiry.
func (s *Signer) Issue(userID, sessionID string, adult bool, roles []string) (string, time.Time, error) {
	now := time.Now()
	exp := now.Add(s.ttl)
	c := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    Issuer,
			Subject:   userID,
			Audience:  jwt.ClaimStrings{Audience},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-5 * time.Second)),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
		SessionID: sessionID,
		Adult:     adult,
		Roles:     roles,
	}
	t := jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
	t.Header["kid"] = s.keys[0].KID
	signed, err := t.SignedString(s.keys[0].Private)
	return signed, exp, err
}

// JWKS renders the public key set.
func (s *Signer) JWKS() []byte {
	type jwk struct {
		Kty string `json:"kty"`
		Crv string `json:"crv"`
		X   string `json:"x"`
		Kid string `json:"kid"`
		Alg string `json:"alg"`
		Use string `json:"use"`
	}
	out := struct {
		Keys []jwk `json:"keys"`
	}{}
	for _, k := range s.keys {
		pub := k.Private.Public().(ed25519.PublicKey)
		out.Keys = append(out.Keys, jwk{"OKP", "Ed25519", base64.RawURLEncoding.EncodeToString(pub), k.KID, "EdDSA", "sig"})
	}
	b, _ := json.Marshal(out)
	return b
}

// ---- verification (every service) ----

// KeySource resolves a key ID to a public key.
type KeySource interface {
	Key(ctx context.Context, kid string) (ed25519.PublicKey, error)
}

// StaticKeys is a fixed key set (tests, or identity verifying its own tokens).
type StaticKeys map[string]ed25519.PublicKey

func (s StaticKeys) Key(_ context.Context, kid string) (ed25519.PublicKey, error) {
	if k, ok := s[kid]; ok {
		return k, nil
	}
	return nil, fmt.Errorf("auth: unknown kid %q", kid)
}

// RemoteJWKS fetches and caches identity's JWKS.
type RemoteJWKS struct {
	URL    string
	Client *http.Client

	mu        sync.RWMutex
	keys      map[string]ed25519.PublicKey
	fetchedAt time.Time
	lastMiss  time.Time
}

func NewRemoteJWKS(url string) *RemoteJWKS {
	return &RemoteJWKS{URL: url, Client: &http.Client{Timeout: 5 * time.Second}}
}

func (r *RemoteJWKS) Key(ctx context.Context, kid string) (ed25519.PublicKey, error) {
	r.mu.RLock()
	k, ok := r.keys[kid]
	fresh := time.Since(r.fetchedAt) < 10*time.Minute
	recentMiss := time.Since(r.lastMiss) < 30*time.Second
	r.mu.RUnlock()
	if ok && fresh {
		return k, nil
	}
	if !ok && recentMiss && fresh {
		return nil, fmt.Errorf("auth: unknown kid %q", kid)
	}
	if err := r.refresh(ctx); err != nil {
		if ok { // serve stale on refresh failure
			return k, nil
		}
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if k, ok := r.keys[kid]; ok {
		return k, nil
	}
	r.lastMiss = time.Now()
	return nil, fmt.Errorf("auth: unknown kid %q", kid)
}

func (r *RemoteJWKS) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return err
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return fmt.Errorf("auth: fetch jwks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("auth: fetch jwks: status %d", resp.StatusCode)
	}
	var set struct {
		Keys []struct {
			Kty, Crv, X, Kid string
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return fmt.Errorf("auth: decode jwks: %w", err)
	}
	keys := map[string]ed25519.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "OKP" || k.Crv != "Ed25519" {
			continue
		}
		b, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil || len(b) != ed25519.PublicKeySize {
			continue
		}
		keys[k.Kid] = ed25519.PublicKey(b)
	}
	if len(keys) == 0 {
		return errors.New("auth: jwks has no usable keys")
	}
	r.mu.Lock()
	r.keys, r.fetchedAt = keys, time.Now()
	r.mu.Unlock()
	return nil
}

// Verifier validates access tokens.
type Verifier struct {
	Keys KeySource
}

var ErrInvalidToken = errors.New("auth: invalid token")

func (v *Verifier) Verify(ctx context.Context, token string) (Principal, error) {
	var c Claims
	_, err := jwt.ParseWithClaims(token, &c, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		return v.Keys.Key(ctx, kid)
	},
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(Issuer),
		jwt.WithAudience(Audience),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30*time.Second),
	)
	if err != nil || c.Subject == "" {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	return Principal{UserID: c.Subject, SessionID: c.SessionID, Adult: c.Adult, Roles: c.Roles, Token: token}, nil
}

// ---- context ----

type ctxKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// BearerToken extracts the token from the Authorization header, or from the
// access_token query parameter when allowQuery is set (browser WebSockets
// cannot send headers).
func BearerToken(r *http.Request, allowQuery bool) string {
	if h := r.Header.Get("Authorization"); len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	if allowQuery {
		return r.URL.Query().Get("access_token")
	}
	return ""
}

// Redis keys checked by the gateway on every request, written by identity on
// logout, password change, ban and account deletion. TTL = access token TTL,
// so the denylist stays small: after that, the token has expired anyway.
func RevokedSessionKey(sid string) string { return "auth:revoked:" + sid }
func BannedUserKey(uid string) string     { return "auth:banned:" + uid }
