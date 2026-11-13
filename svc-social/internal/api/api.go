// Package api is svc-social's HTTP layer.
package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-social/internal/app"
)

type API struct {
	Svc           *app.Service
	Verifier      *auth.Verifier
	InternalToken string
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	priv := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(true, false)(f) }
	opt := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(false, false)(f) }
	internal := func(f httpx.Handler) http.Handler { return httpx.InternalOnly(a.InternalToken)(f) }

	mux.Handle("POST /v1/social/follows/{user_id}", priv(a.follow(true)))
	mux.Handle("DELETE /v1/social/follows/{user_id}", priv(a.follow(false)))
	mux.Handle("GET /v1/social/users/{id}/followers", opt(a.list("followers")))
	mux.Handle("GET /v1/social/users/{id}/following", opt(a.list("following")))
	mux.Handle("GET /v1/social/users/{id}/counts", opt(a.userCounts))
	mux.Handle("GET /v1/social/users/{id}/relationship", priv(a.relationship))
	mux.Handle("POST /v1/social/videos/{id}/like", priv(a.like(true)))
	mux.Handle("DELETE /v1/social/videos/{id}/like", priv(a.like(false)))
	mux.Handle("GET /v1/social/videos/counts", opt(a.videoCounts))
	mux.Handle("GET /v1/social/videos/{id}/comments", opt(a.comments))
	mux.Handle("POST /v1/social/videos/{id}/comments", priv(a.comment))
	mux.Handle("DELETE /v1/social/comments/{id}", priv(a.deleteComment))
	mux.Handle("POST /v1/social/blocks/{user_id}", priv(a.block(true)))
	mux.Handle("DELETE /v1/social/blocks/{user_id}", priv(a.block(false)))
	mux.Handle("POST /v1/social/reports", priv(a.report))

	mux.Handle("GET /internal/social/users/{id}/follower-ids", internal(a.followerIDs))
	mux.Handle("GET /internal/social/users/{id}/following-ids", internal(a.followingIDs))
	return httpx.Chain(mux, httpx.Timeout(10*time.Second))
}

func me(r *http.Request) string {
	if p, ok := auth.FromContext(r.Context()); ok {
		return p.UserID
	}
	return ""
}

func (a *API) follow(on bool) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		p, _ := auth.User(r)
		var err error
		if on {
			err = a.Svc.Follow(r.Context(), p.UserID, r.PathValue("user_id"))
		} else {
			err = a.Svc.Unfollow(r.Context(), p.UserID, r.PathValue("user_id"))
		}
		if err != nil {
			return err
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
}

func (a *API) list(dir string) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		items, next, err := a.Svc.List(r.Context(), r.PathValue("id"), dir, r.URL.Query().Get("cursor"), limit)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusOK, httpx.Page[app.Edge]{Items: items, NextCursor: next})
		return nil
	}
}

func (a *API) userCounts(w http.ResponseWriter, r *http.Request) error {
	c, err := a.Svc.UserCounts(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "public, max-age=10")
	httpx.JSON(w, http.StatusOK, c)
	return nil
}

func (a *API) relationship(w http.ResponseWriter, r *http.Request) error {
	rel, err := a.Svc.Relationship(r.Context(), me(r), r.PathValue("id"))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, rel)
	return nil
}

func (a *API) like(on bool) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		if err := a.Svc.Like(r.Context(), me(r), r.PathValue("id"), on); err != nil {
			return err
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
}

func (a *API) videoCounts(w http.ResponseWriter, r *http.Request) error {
	items, err := a.Svc.VideoCounts(r.Context(), me(r), strings.Split(r.URL.Query().Get("ids"), ","))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": items})
	return nil
}

func (a *API) comments(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	items, next, err := a.Svc.Comments(r.Context(), r.PathValue("id"), q.Get("parent_id"), q.Get("cursor"), limit)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, httpx.Page[app.Comment]{Items: items, NextCursor: next})
	return nil
}

func (a *API) comment(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Text     string `json:"text"`
		ParentID string `json:"parent_id,omitempty"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	c, err := a.Svc.Comment(r.Context(), me(r), r.PathValue("id"), in.ParentID, in.Text)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusCreated, c)
	return nil
}

func (a *API) deleteComment(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.DeleteComment(r.Context(), p.UserID, r.PathValue("id"), p.HasRole("moderator")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) block(on bool) httpx.Handler {
	return func(w http.ResponseWriter, r *http.Request) error {
		if err := a.Svc.Block(r.Context(), me(r), r.PathValue("user_id"), on); err != nil {
			return err
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
}

func (a *API) report(w http.ResponseWriter, r *http.Request) error {
	var in events.ReportData
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	id, err := a.Svc.Report(r.Context(), me(r), in)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusAccepted, map[string]string{"report_id": id})
	return nil
}

func (a *API) followerIDs(w http.ResponseWriter, r *http.Request) error {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ids, err := a.Svc.FollowerIDs(r.Context(), r.PathValue("id"), r.URL.Query().Get("after"), limit)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"ids": ids})
	return nil
}

func (a *API) followingIDs(w http.ResponseWriter, r *http.Request) error {
	ids, err := a.Svc.FollowingIDs(r.Context(), r.PathValue("id"), 1000)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"ids": ids})
	return nil
}
