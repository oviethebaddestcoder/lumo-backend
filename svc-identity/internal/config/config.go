// Package config loads svc-identity settings from the environment.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	kitcfg "github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
)

type OIDC struct {
	Issuers   []string
	Audiences []string
	JWKSURL   string
	Nonce     bool // require a nonce (Apple)
}

type Config struct {
	Addr        string
	LogLevel    string
	DatabaseURL string
	DirectURL   string // non-pooled, for migrations
	RedisURL    string
	RedisNodes  string
	Kafka       kafkax.Config

	AccessTTL     time.Duration
	RefreshTTL    time.Duration // sliding
	SessionMaxAge time.Duration // absolute
	SigningKeys   []auth.SigningKey

	OTPSecret      []byte
	OTPTestNumbers map[string]string // fixed codes for app-store reviewers
	OTPPerIPHour   int               // codes per client IP per hour (carrier NAT-aware)
	DefaultRegion  string            // for phone parsing, e.g. "NG"
	SMSProvider    string            // log | termii | twilio
	TermiiKey      string
	TermiiSender   string
	TwilioSID      string
	TwilioToken    string
	TwilioFrom     string
	EmailProvider  string // log | resend
	ResendKey      string
	EmailFrom      string

	Google   OIDC
	Apple    OIDC
	Line     OIDC
	Kakao    OIDC
	FBLimitd OIDC

	FacebookAppID     string
	FacebookAppSecret string
	FacebookGraphURL  string

	XClientID     string
	XClientSecret string
}

func Load() (Config, error) {
	e := kitcfg.New()
	c := Config{
		Addr:        e.Str("ADDR", ":8080"),
		LogLevel:    e.Str("LOG_LEVEL", "info"),
		DatabaseURL: e.Required("DATABASE_URL"),
		DirectURL:   e.Str("DATABASE_URL_DIRECT", ""),
		RedisURL:    e.Str("REDIS_URL", "redis://localhost:6379/0"),
		RedisNodes:  e.Str("REDIS_CLUSTER_ADDRS", ""),
		Kafka:       kafkax.ConfigFromEnv(e, "svc-identity"),

		AccessTTL:     e.Duration("ACCESS_TOKEN_TTL", 15*time.Minute),
		RefreshTTL:    e.Duration("REFRESH_TOKEN_TTL", 60*24*time.Hour),
		SessionMaxAge: e.Duration("SESSION_MAX_AGE", 365*24*time.Hour),

		OTPSecret:     e.Base64("OTP_HMAC_SECRET", true),
		DefaultRegion: e.Str("PHONE_DEFAULT_REGION", "NG"),
		SMSProvider:   e.Str("SMS_PROVIDER", "log"),
		TermiiKey:     e.Str("TERMII_API_KEY", ""),
		TermiiSender:  e.Str("TERMII_SENDER_ID", "Lumo"),
		TwilioSID:     e.Str("TWILIO_ACCOUNT_SID", ""),
		TwilioToken:   e.Str("TWILIO_AUTH_TOKEN", ""),
		TwilioFrom:    e.Str("TWILIO_FROM", ""),
		EmailProvider: e.Str("EMAIL_PROVIDER", "log"),
		ResendKey:     e.Str("RESEND_API_KEY", ""),
		EmailFrom:     e.Str("EMAIL_FROM", "Lumo <no-reply@example.com>"),

		Google: OIDC{
			Issuers:   []string{"https://accounts.google.com", "accounts.google.com"},
			Audiences: e.List("GOOGLE_CLIENT_IDS", nil), // iOS, Android and web client IDs
			JWKSURL:   e.Str("GOOGLE_JWKS_URL", "https://www.googleapis.com/oauth2/v3/certs"),
		},
		Apple: OIDC{
			Issuers:   []string{"https://appleid.apple.com"},
			Audiences: e.List("APPLE_CLIENT_IDS", nil), // bundle ID (native) + services ID (web)
			JWKSURL:   e.Str("APPLE_JWKS_URL", "https://appleid.apple.com/auth/keys"),
			Nonce:     true,
		},
		Line: OIDC{
			Issuers:   []string{"https://access.line.me"},
			Audiences: e.List("LINE_CHANNEL_IDS", nil),
			JWKSURL:   e.Str("LINE_JWKS_URL", "https://api.line.me/oauth2/v2.1/certs"),
		},
		Kakao: OIDC{
			Issuers:   []string{"https://kauth.kakao.com"},
			Audiences: e.List("KAKAO_APP_KEYS", nil),
			JWKSURL:   e.Str("KAKAO_JWKS_URL", "https://kauth.kakao.com/.well-known/jwks.json"),
		},
		FBLimitd: OIDC{ // Facebook Limited Login (iOS) issues OIDC tokens
			Issuers:   []string{"https://www.facebook.com"},
			Audiences: e.List("FACEBOOK_APP_IDS", nil),
			JWKSURL:   e.Str("FACEBOOK_JWKS_URL", "https://limited.facebook.com/.well-known/oauth/openid/jwks/"),
			Nonce:     true,
		},
		FacebookAppID:     e.Str("FACEBOOK_APP_ID", ""),
		FacebookAppSecret: e.Str("FACEBOOK_APP_SECRET", ""),
		FacebookGraphURL:  e.Str("FACEBOOK_GRAPH_URL", "https://graph.facebook.com"),
		XClientID:         e.Str("X_CLIENT_ID", ""),
		XClientSecret:     e.Str("X_CLIENT_SECRET", ""),
	}
	if c.DirectURL == "" {
		c.DirectURL = c.DatabaseURL
	}

	// SIGNING_KEYS: "kid1:<base64 PEM>,kid2:<base64 PEM>" — first signs, all verify.
	for _, part := range e.List("SIGNING_KEYS", nil) {
		kid, b64, ok := strings.Cut(part, ":")
		if !ok {
			return c, fmt.Errorf("config: SIGNING_KEYS entry must be kid:base64pem")
		}
		pemBytes := kitcfg.DecodeBase64(b64)
		pk, err := auth.ParsePrivateKeyPEM(pemBytes)
		if err != nil {
			return c, fmt.Errorf("config: signing key %s: %w", kid, err)
		}
		c.SigningKeys = append(c.SigningKeys, auth.SigningKey{KID: kid, Private: pk})
	}
	if len(c.SigningKeys) == 0 {
		e.Required("SIGNING_KEYS")
	}

	c.OTPPerIPHour = e.Int("OTP_PER_IP_PER_HOUR", 200)
	c.OTPTestNumbers = map[string]string{}
	for _, p := range e.List("OTP_TEST_ACCOUNTS", nil) { // "+2348000000001=123456,review@example.com=654321"
		if k, v, ok := strings.Cut(p, "="); ok {
			c.OTPTestNumbers[strings.ToLower(k)] = v
		}
	}
	if len(c.OTPSecret) > 0 && len(c.OTPSecret) < 32 {
		return c, fmt.Errorf("config: OTP_HMAC_SECRET must be at least 32 bytes")
	}
	return c, e.Err()
}
