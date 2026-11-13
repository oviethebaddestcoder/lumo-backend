package auth

import (
	"net/http"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
)

// Middleware authenticates requests. With required=false, anonymous requests
// pass through (public reads) but a bad token is still rejected.
func (v *Verifier) Middleware(required, allowQuery bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := BearerToken(r, allowQuery)
			if tok == "" {
				if required {
					httpx.Error(w, r, errs.ErrUnauthenticated)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			p, err := v.Verify(r.Context(), tok)
			if err != nil {
				httpx.Error(w, r, errs.Unauthorized("invalid_token", "Your session has expired. Sign in again.").Wrap(err))
				return
			}
			ctx := WithPrincipal(r.Context(), p)
			ctx = logx.With(ctx, "user_id", p.UserID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// User returns the principal or an unauthenticated error.
func User(r *http.Request) (Principal, error) {
	p, ok := FromContext(r.Context())
	if !ok {
		return Principal{}, errs.ErrUnauthenticated
	}
	return p, nil
}

// Adult returns the principal only if the user is 18+.
func Adult(r *http.Request) (Principal, error) {
	p, err := User(r)
	if err != nil {
		return p, err
	}
	if !p.Adult {
		return p, errs.ErrAdultOnly
	}
	return p, nil
}

// Staff returns the principal only if it has the given role.
func Role(r *http.Request, role string) (Principal, error) {
	p, err := User(r)
	if err != nil {
		return p, err
	}
	if !p.HasRole(role) {
		return p, errs.ErrForbidden
	}
	return p, nil
}
