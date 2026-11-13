// Package api is the HTTP surface of svc-messaging.
package api

import (
	"net/http"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-messaging/internal/app"
)

type API struct {
	Svc      *app.Service
	Verifier *auth.Verifier
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	priv := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(true, false)(f) }
	mux.Handle("GET /v1/dm/conversations", priv(a.list))
	mux.Handle("POST /v1/dm/conversations", priv(a.open))
	mux.Handle("GET /v1/dm/conversations/{id}", priv(a.get))
	mux.Handle("DELETE /v1/dm/conversations/{id}", priv(a.hide))
	mux.Handle("GET /v1/dm/conversations/{id}/messages", priv(a.messages))
	mux.Handle("POST /v1/dm/conversations/{id}/messages", priv(a.send))
	mux.Handle("POST /v1/dm/conversations/{id}/read", priv(a.read))
	mux.Handle("DELETE /v1/dm/messages/{id}", priv(a.unsend))
	mux.Handle("GET /v1/dm/unread", priv(a.unread))
	return httpx.Chain(mux, httpx.Timeout(10*time.Second))
}

func (a *API) list(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	items, next, err := a.Svc.Conversations(r.Context(), p.UserID, r.URL.Query().Get("cursor"), 0)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, httpx.Page[app.Conversation]{Items: items, NextCursor: next})
	return nil
}

func (a *API) open(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		UserID string `json:"user_id"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	c, err := a.Svc.Open(r.Context(), p.UserID, in.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, c)
	return nil
}

func (a *API) get(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	c, err := a.Svc.Get(r.Context(), p.UserID, r.PathValue("id"))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, c)
	return nil
}

func (a *API) hide(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.Hide(r.Context(), p.UserID, r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) messages(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	items, next, err := a.Svc.Messages(r.Context(), p.UserID, r.PathValue("id"), r.URL.Query().Get("cursor"), 0)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, httpx.Page[app.Message]{Items: items, NextCursor: next})
	return nil
}

func (a *API) send(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		Text     string `json:"text"`
		ClientID string `json:"client_id"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	m, err := a.Svc.Send(r.Context(), p.UserID, r.PathValue("id"), in.ClientID, in.Text)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusCreated, m)
	return nil
}

func (a *API) read(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.MarkRead(r.Context(), p.UserID, r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) unsend(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.Unsend(r.Context(), p.UserID, r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) unread(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	n, err := a.Svc.Unread(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]int{"count": n})
	return nil
}
