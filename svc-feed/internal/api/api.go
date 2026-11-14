// Package api is svc-feed's HTTP layer.
package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-feed/internal/app"
)

type API struct {
	Svc      *app.Service
	Verifier *auth.Verifier
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	opt := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(false, false)(f) }
	priv := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(true, false)(f) }
	mux.Handle("GET /v1/feed/foryou", opt(a.forYou("short")))
	mux.Handle("GET /v1/feed/watch", opt(a.forYou("long")))
	mux.Handle("GET /v1/feed/following", priv(a.following))
	mux.Handle("POST /v1/feed/impressions", priv(a.impressions))
	mux.Handle("POST /v1/feed/not-interested", priv(a.notInterested))
	mux.Handle("GET /v1/feed/me/interests", priv(a.interests))
	mux.Handle("DELETE /v1/feed/me/interests/{tag}", priv(a.removeInterest))
	mux.Handle("POST /v1/feed/me/reset", priv(a.reset))
	return httpx.Chain(mux, httpx.Timeout(5*time.Second))
}

func (a *API) forYou(kind string) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		uid := ""
		if p, ok := auth.FromContext(r.Context()); ok {
			uid = p.UserID
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		items, err := a.Svc.ForYou(r.Context(), uid, kind, limit)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusOK, map[string]any{"items": items})
		return nil
	}
}

func (a *API) following(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	before, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
	items, next, err := a.Svc.Following(r.Context(), p.UserID, before, 10)
	if err != nil {
		return err
	}
	page := httpx.Page[app.Item]{Items: items}
	if next > 0 {
		page.NextCursor = strconv.FormatInt(next, 10)
	}
	httpx.JSON(w, http.StatusOK, page)
	return nil
}

func (a *API) impressions(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		Items []events.ImpressionData `json:"items"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	a.Svc.Impressions(r.Context(), p.UserID, in.Items)
	w.WriteHeader(http.StatusAccepted)
	return nil
}

func (a *API) notInterested(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		VideoID string `json:"video_id"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	if len(in.VideoID) != 36 {
		return errs.BadRequest("video_invalid", "Give a video.")
	}
	if err := a.Svc.NotInterested(r.Context(), p.UserID, in.VideoID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) interests(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	items, err := a.Svc.Interests(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": items})
	return nil
}

func (a *API) removeInterest(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	tag := r.PathValue("tag")
	if tag == "" || len(tag) > 50 {
		return errs.BadRequest("tag_invalid", "Give a hashtag.")
	}
	if err := a.Svc.RemoveInterest(r.Context(), p.UserID, tag); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) reset(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.Reset(r.Context(), p.UserID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
