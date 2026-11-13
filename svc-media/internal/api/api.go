// Package api is svc-media's HTTP layer.
package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/s3x"
	"github.com/oviethebaddestcoder/lumo-backend/svc-media/internal/app"
)

type API struct {
	Svc      *app.Service
	Verifier *auth.Verifier
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	priv := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(true, false)(f) }
	opt := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(false, false)(f) }
	mux.Handle("POST /v1/media/uploads", priv(a.create))
	mux.Handle("POST /v1/media/uploads/{id}/parts", priv(a.parts))
	mux.Handle("POST /v1/media/uploads/{id}/complete", priv(a.complete))
	mux.Handle("GET /v1/media/videos/{id}", opt(a.get))
	mux.Handle("PATCH /v1/media/videos/{id}", priv(a.update))
	mux.Handle("DELETE /v1/media/videos/{id}", priv(a.remove))
	mux.Handle("GET /v1/media/users/{id}/videos", opt(a.byOwner))
	return httpx.Chain(mux, httpx.Timeout(20*time.Second))
}

func viewer(r *http.Request) string {
	if p, ok := auth.FromContext(r.Context()); ok {
		return p.UserID
	}
	return ""
}

func (a *API) create(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in app.UploadInput
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	u, err := a.Svc.CreateUpload(r.Context(), p, in)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusCreated, u)
	return nil
}

func (a *API) parts(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		From int `json:"from"`
		To   int `json:"to"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	urls, err := a.Svc.PartURLs(r.Context(), p, r.PathValue("id"), in.From, in.To)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"from": in.From, "part_urls": urls})
	return nil
}

func (a *API) complete(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		Parts []s3x.Part `json:"parts"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	v, err := a.Svc.Complete(r.Context(), p, r.PathValue("id"), in.Parts)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusAccepted, v)
	return nil
}

func (a *API) get(w http.ResponseWriter, r *http.Request) error {
	v, err := a.Svc.Get(r.Context(), viewer(r), r.PathValue("id"))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, v)
	return nil
}

func (a *API) update(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in app.Patch
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	v, err := a.Svc.Update(r.Context(), p, r.PathValue("id"), in)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, v)
	return nil
}

func (a *API) remove(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.Remove(r.Context(), r.PathValue("id"), p.UserID, "owner"); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) byOwner(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	items, next, err := a.Svc.ByOwner(r.Context(), viewer(r), r.PathValue("id"), q.Get("kind"), q.Get("cursor"), limit)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, httpx.Page[app.Video]{Items: items, NextCursor: next})
	return nil
}
