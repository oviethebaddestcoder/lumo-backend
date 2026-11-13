// Package api is svc-live's HTTP layer.
package api

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-live/internal/livekit"
)

type API struct {
	Svc      *app.Service
	Verifier *auth.Verifier
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	priv := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(true, false)(f) }
	opt := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(false, false)(f) }

	mux.Handle("GET /v1/live/rooms", opt(a.list))
	mux.Handle("GET /v1/live/rooms/{id}", opt(a.get))
	mux.Handle("POST /v1/live/rooms", priv(a.start))
	mux.Handle("POST /v1/live/rooms/{id}/join", priv(a.join))
	mux.Handle("POST /v1/live/rooms/{id}/end", priv(a.end))
	mux.Handle("GET /v1/live/stream-key", priv(a.streamKey))
	mux.Handle("POST /v1/live/stream-key/rotate", priv(a.rotateKey))
	mux.Handle("POST /v1/live/pk/invite", priv(a.pkInvite))
	mux.Handle("POST /v1/live/pk/{id}/accept", priv(a.pkAccept))
	mux.Handle("POST /v1/live/pk/{id}/decline", priv(a.pkDecline))
	mux.Handle("GET /v1/live/pk/{id}", priv(a.pkGet))
	mux.Handle("GET /v1/live/categories", opt(func(w http.ResponseWriter, r *http.Request) error {
		httpx.JSON(w, http.StatusOK, map[string]any{"items": app.Categories})
		return nil
	}))
	mux.Handle("POST /v1/live/webhooks/livekit", httpx.Handler(a.webhook))
	return httpx.Chain(mux, httpx.Timeout(15*time.Second))
}

func (a *API) list(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	off, _ := strconv.Atoi(q.Get("cursor"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	rooms, err := a.Svc.List(r.Context(), q.Get("category"), off, limit)
	if err != nil {
		return err
	}
	page := httpx.Page[app.Room]{Items: rooms}
	if len(rooms) > 0 {
		page.NextCursor = strconv.Itoa(off + len(rooms))
	}
	w.Header().Set("Cache-Control", "public, max-age=3")
	httpx.JSON(w, http.StatusOK, page)
	return nil
}

func (a *API) get(w http.ResponseWriter, r *http.Request) error {
	room, err := a.Svc.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, room)
	return nil
}

func (a *API) start(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in app.StartInput
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	room, conn, err := a.Svc.Start(r.Context(), p, in)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusCreated, map[string]any{"room": room, "connection": conn})
	return nil
}

func (a *API) join(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	conn, err := a.Svc.Join(r.Context(), p.UserID, r.PathValue("id"))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, conn)
	return nil
}

func (a *API) end(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.End(r.Context(), r.PathValue("id"), "host_ended", p.UserID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) streamKey(w http.ResponseWriter, r *http.Request) error {
	p, err := auth.Adult(r)
	if err != nil {
		return err
	}
	k, err := a.Svc.StreamKey(r.Context(), p, false)
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.JSON(w, http.StatusOK, k)
	return nil
}

func (a *API) rotateKey(w http.ResponseWriter, r *http.Request) error {
	p, err := auth.Adult(r)
	if err != nil {
		return err
	}
	k, err := a.Svc.StreamKey(r.Context(), p, true)
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.JSON(w, http.StatusOK, k)
	return nil
}

func (a *API) pkInvite(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		ToRoomID string `json:"to_room_id"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	v, err := a.Svc.Invite(r.Context(), p, in.ToRoomID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusCreated, v)
	return nil
}

func (a *API) pkAccept(w http.ResponseWriter, r *http.Request) error { return a.pkRespond(w, r, true) }
func (a *API) pkDecline(w http.ResponseWriter, r *http.Request) error {
	return a.pkRespond(w, r, false)
}

func (a *API) pkRespond(w http.ResponseWriter, r *http.Request, accept bool) error {
	p, _ := auth.User(r)
	v, err := a.Svc.Respond(r.Context(), p, r.PathValue("id"), accept)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, v)
	return nil
}

func (a *API) pkGet(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	v, err := a.Svc.CrossToken(r.Context(), p, r.PathValue("id"))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, v)
	return nil
}

func (a *API) webhook(w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	ev, err := a.Svc.LK.VerifyWebhook(body, r.Header.Get("Authorization"))
	if errors.Is(err, livekit.ErrBadWebhook) {
		return errs.Unauthorized("bad_signature", "Invalid webhook signature.").Wrap(err)
	}
	if err != nil {
		return errs.BadRequest("bad_webhook", "Bad webhook body.")
	}
	if err := a.Svc.HandleWebhook(r.Context(), ev); err != nil {
		return err // 5xx: LiveKit retries
	}
	w.WriteHeader(http.StatusOK)
	return nil
}
