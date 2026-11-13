// Package api is svc-moderation's HTTP layer. Every route needs the
// "moderator" role in the access token (set in identity's users.roles).
package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-moderation/internal/app"
)

const role = "moderator"

type API struct {
	Svc      *app.Service
	Verifier *auth.Verifier
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mod := func(f httpx.Handler) http.Handler {
		return a.Verifier.Middleware(true, false)(httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
			if _, err := auth.Role(r, role); err != nil {
				return err
			}
			return f(w, r)
		}))
	}
	mux.Handle("GET /v1/moderation/queue", mod(a.queue))
	mux.Handle("POST /v1/moderation/actions", mod(a.act))
	mux.Handle("POST /v1/moderation/targets/{type}/{id}/dismiss", mod(a.dismiss))
	mux.Handle("GET /v1/moderation/targets/{type}/{id}/history", mod(a.history))
	return httpx.Chain(mux, httpx.Timeout(10*time.Second))
}

func (a *API) queue(w http.ResponseWriter, r *http.Request) error {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := a.Svc.Queue(r.Context(), limit)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": items})
	return nil
}

func (a *API) act(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in app.ActionInput
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	id, err := a.Svc.Act(r.Context(), p, in)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusCreated, map[string]string{"action_id": id})
	return nil
}

func (a *API) dismiss(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.Dismiss(r.Context(), p, r.PathValue("type"), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) history(w http.ResponseWriter, r *http.Request) error {
	items, err := a.Svc.History(r.Context(), r.PathValue("type"), r.PathValue("id"))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": items})
	return nil
}
