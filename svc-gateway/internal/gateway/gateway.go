// Package gateway is the single public entry point for the apps and web.
//
// Per request it: resolves the real client IP, enforces CORS and body
// limits, verifies the access token (if present) and checks the session
// denylist, applies per-user / per-IP rate limits in Redis, then proxies to
// the owning service by longest path prefix. WebSocket upgrades pass through.
//
// Services still verify the JWT themselves (defense in depth); the gateway's
// job is to reject bad traffic early and enforce revocation instantly.
package gateway

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/redis/go-redis/v9"
)

type Route struct {
	Prefix   string
	Upstream *url.URL
	proxy    *httputil.ReverseProxy
}

type Limits struct {
	PerUser   int // requests per minute per signed-in user
	PerIP     int // requests per minute per IP (anonymous)
	AuthPerIP int // requests per minute per IP on /v1/auth
}

type Gateway struct {
	routes         []*Route
	verifier       *auth.Verifier
	redis          redis.UniversalClient
	limiter        redisx.Limiter
	limits         Limits
	allowedOrigins []string
	trusted        []*net.IPNet
}

var (
	denied       = metrics.NewCounterVec("gateway_denied_total", "Requests rejected at the gateway.", "reason")
	upstreamErrs = metrics.NewCounterVec("gateway_upstream_errors_total", "Upstream proxy errors.", "upstream")
)

// ParseRoutes reads "prefix=url,prefix=url".
func ParseRoutes(spec string) ([]*Route, error) {
	var rs []*Route
	for _, p := range strings.Split(spec, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		prefix, raw, ok := strings.Cut(p, "=")
		u, err := url.Parse(raw)
		if !ok || err != nil || u.Scheme == "" || u.Host == "" || !strings.HasPrefix(prefix, "/") {
			return nil, errs.BadRequest("bad_route", "invalid route "+p)
		}
		rs = append(rs, &Route{Prefix: prefix, Upstream: u})
	}
	// Longest prefix first.
	sort.Slice(rs, func(i, j int) bool { return len(rs[i].Prefix) > len(rs[j].Prefix) })
	return rs, nil
}

func New(routes []*Route, v *auth.Verifier, rdb redis.UniversalClient, limits Limits, origins []string, trustedCIDRs []string) (*Gateway, error) {
	g := &Gateway{routes: routes, verifier: v, redis: rdb, limiter: redisx.Limiter{R: rdb, Prefix: "rl:gw:"},
		limits: limits, allowedOrigins: origins}
	for _, c := range trustedCIDRs {
		_, n, err := net.ParseCIDR(strings.TrimSpace(c))
		if err != nil {
			return nil, err
		}
		g.trusted = append(g.trusted, n)
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          4096,
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ForceAttemptHTTP2:     false,
	}
	for _, r := range routes {
		target := r.Upstream
		name := target.Host
		r.proxy = &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(target)
				pr.Out.Host = target.Host
				ip, _ := pr.In.Context().Value(clientIPKey{}).(string)
				pr.Out.Header.Set("X-Forwarded-For", ip)
				pr.Out.Header.Set("X-Real-IP", ip)
				pr.Out.Header.Set("X-Forwarded-Proto", "https")
				pr.Out.Header.Del("X-User-Id")
			},
			Transport:     transport,
			FlushInterval: -1, // stream (SSE / long poll) without buffering
			ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
				upstreamErrs.WithLabelValues(name).Inc()
				slog.ErrorContext(req.Context(), "upstream error", "upstream", name, "err", err)
				code := http.StatusBadGateway
				if req.Context().Err() != nil {
					code = 499
				}
				httpx.JSON(w, code, map[string]any{"error": map[string]string{"code": "upstream_unavailable", "message": "Service is temporarily unavailable."}})
			},
		}
	}
	return g, nil
}

type clientIPKey struct{}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ip := g.clientIP(r)
	r = r.WithContext(context.WithValue(r.Context(), clientIPKey{}, ip))
	setSecurityHeaders(w)

	if !g.cors(w, r) {
		return
	}
	if !canonicalPath(r) {
		denied.WithLabelValues("bad_path").Inc()
		httpx.Error(w, r, errs.BadRequest("bad_path", "Invalid request path."))
		return
	}
	route := g.match(r.URL.Path)
	if route == nil {
		denied.WithLabelValues("no_route").Inc()
		httpx.Error(w, r, errs.ErrNotFound)
		return
	}

	isWS := strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
	if !isWS && r.ContentLength > 2<<20 {
		denied.WithLabelValues("body_too_large").Inc()
		httpx.Error(w, r, errs.New(http.StatusRequestEntityTooLarge, "body_too_large", "Request body is too large."))
		return
	}

	userKey := ""
	if tok := auth.BearerToken(r, isWS); tok != "" {
		p, err := g.verifier.Verify(r.Context(), tok)
		if err != nil {
			denied.WithLabelValues("invalid_token").Inc()
			httpx.Error(w, r, errs.Unauthorized("invalid_token", "Your session has expired. Sign in again."))
			return
		}
		if g.revoked(r.Context(), p) {
			denied.WithLabelValues("revoked").Inc()
			httpx.Error(w, r, errs.Unauthorized("session_revoked", "You were signed out. Sign in again."))
			return
		}
		userKey = "u:" + p.UserID
		if isWS && r.Header.Get("Authorization") == "" {
			// Browser WebSockets send the token in the query; hand it to the
			// service as a header and keep it out of upstream access logs.
			r.Header.Set("Authorization", "Bearer "+tok)
			q := r.URL.Query()
			q.Del("access_token")
			r.URL.RawQuery = q.Encode()
		}
	}

	if !g.allow(w, r, ip, userKey) {
		return
	}
	route.proxy.ServeHTTP(w, r)
}

// canonicalPath rejects paths that could be read differently upstream:
// dot segments, doubled slashes, backslashes, and encoded "/" or "\"
// (so /v1/media/..%2f..%2finternal can't match one route here and mean
// another inside a service).
func canonicalPath(r *http.Request) bool {
	p := r.URL.Path
	if p == "" || p[0] != '/' || strings.ContainsAny(p, "\\\x00") {
		return false
	}
	clean := path.Clean(p)
	if clean != p && clean+"/" != p {
		return false
	}
	raw := strings.ToLower(r.URL.EscapedPath())
	return !strings.Contains(raw, "%2f") && !strings.Contains(raw, "%5c") && !strings.Contains(raw, "%00")
}

func (g *Gateway) match(path string) *Route {
	for _, r := range g.routes {
		if path == r.Prefix || strings.HasPrefix(path, strings.TrimSuffix(r.Prefix, "/")+"/") {
			return r
		}
	}
	return nil
}

// revoked checks the denylist written by svc-identity. If Redis is down we
// fail open: tokens still expire within 15 minutes, and an outage of the
// whole API is worse than that window.
func (g *Gateway) revoked(ctx context.Context, p auth.Principal) bool {
	ctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	n, err := g.redis.Exists(ctx, auth.RevokedSessionKey(p.SessionID), auth.BannedUserKey(p.UserID)).Result()
	if err != nil {
		slog.WarnContext(ctx, "denylist check failed (allowing)", "err", err)
		return false
	}
	return n > 0
}

func (g *Gateway) allow(w http.ResponseWriter, r *http.Request, ip, userKey string) bool {
	ctx, cancel := context.WithTimeout(r.Context(), 150*time.Millisecond)
	defer cancel()
	key, limit := "ip:"+ip, g.limits.PerIP
	if strings.HasPrefix(r.URL.Path, "/v1/auth/") {
		key, limit = "auth:"+ip, g.limits.AuthPerIP
	} else if userKey != "" {
		key, limit = userKey, g.limits.PerUser
	}
	res, err := g.limiter.Allow(ctx, key, limit, time.Minute)
	if err != nil {
		return true // fail open; services have their own limits on sensitive paths
	}
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limit))
	if !res.Allowed {
		denied.WithLabelValues("rate_limited").Inc()
		w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())+1))
		httpx.Error(w, r, errs.ErrRateLimited)
		return false
	}
	return true
}

// clientIP walks X-Forwarded-For from the right, skipping trusted proxies
// (our load balancer), so a client can't spoof its IP by sending the header.
func (g *Gateway) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if !g.isTrusted(host) {
		return host
	}
	hops := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		h := strings.TrimSpace(hops[i])
		if h == "" {
			continue
		}
		if !g.isTrusted(h) {
			return h
		}
	}
	return host
}

func (g *Gateway) isTrusted(ip string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range g.trusted {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}

func (g *Gateway) cors(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	if !slices.Contains(g.allowedOrigins, origin) {
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusForbidden)
			return false
		}
		return true // non-browser or same-origin; CORS headers simply omitted
	}
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Vary", "Origin")
	h.Set("Access-Control-Allow-Credentials", "true")
	if r.Method == http.MethodOptions {
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID, Idempotency-Key, traceparent")
		h.Set("Access-Control-Max-Age", "600")
		w.WriteHeader(http.StatusNoContent)
		return false
	}
	return true
}

func setSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
}
