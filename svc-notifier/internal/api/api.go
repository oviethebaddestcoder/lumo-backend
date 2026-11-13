// Package api is svc-notifier's HTTP layer.
package api

import (
	"net/http"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-notifier/internal/app"
)

type API struct {
	Svc      *app.Service
	Verifier *auth.Verifier
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	priv := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(true, false)(f) }
	mux.Handle("PUT /v1/notify/devices", priv(a.register))
	mux.Handle("DELETE /v1/notify/devices/{token}", priv(a.unregister))
	mux.Handle("GET /v1/notify/prefs", priv(a.getPrefs))
	mux.Handle("PUT /v1/notify/prefs", priv(a.setPrefs))
	return httpx.Chain(mux, httpx.Timeout(5*time.Second))
}

func (a *API) register(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		Token    string `json:"token"`
		Platform string `json:"platform"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	if err := a.Svc.Register(r.Context(), p.UserID, in.Token, in.Platform); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) unregister(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.Unregister(r.Context(), p.UserID, r.PathValue("token")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) getPrefs(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	pr, err := a.Svc.GetPrefs(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, pr)
	return nil
}

func (a *API) setPrefs(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in app.Prefs
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	if err := a.Svc.SetPrefs(r.Context(), p.UserID, in); err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, in)
	return nil
}
