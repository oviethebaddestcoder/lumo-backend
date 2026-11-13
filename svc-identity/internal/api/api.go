// Package api is svc-identity's HTTP layer: decode, call app, encode.
package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-identity/internal/providers"
)

type API struct {
	Svc      *app.Service
	Verifier *auth.Verifier
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	authed := a.Verifier.Middleware(true, false)
	h := func(f httpx.Handler) http.Handler { return f }
	priv := func(f httpx.Handler) http.Handler { return authed(f) }

	mux.HandleFunc("GET /.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		_, _ = w.Write(a.Svc.Signer.JWKS())
	})

	mux.Handle("POST /v1/auth/otp/send", h(a.otpSend))
	mux.Handle("POST /v1/auth/otp/verify", h(a.otpVerify))
	mux.Handle("POST /v1/auth/oauth/{provider}", h(a.oauth))
	mux.Handle("POST /v1/auth/password/login", h(a.passwordLogin))
	mux.Handle("POST /v1/auth/refresh", h(a.refresh))
	mux.Handle("POST /v1/auth/qr", h(a.qrCreate))
	mux.Handle("GET /v1/auth/qr/{id}", h(a.qrPoll))

	mux.Handle("POST /v1/auth/logout", priv(a.logout))
	mux.Handle("POST /v1/auth/logout-all", priv(a.logoutAll))
	mux.Handle("GET /v1/auth/qr/{id}/info", priv(a.qrInfo))
	mux.Handle("POST /v1/auth/qr/{id}/approve", priv(a.qrApprove))

	mux.Handle("GET /v1/me", priv(a.me))
	mux.Handle("PATCH /v1/me", priv(a.updateMe))
	mux.Handle("DELETE /v1/me", priv(a.deleteMe))
	mux.Handle("PUT /v1/me/password", priv(a.setPassword))
	mux.Handle("GET /v1/me/sessions", priv(a.sessions))
	mux.Handle("DELETE /v1/me/sessions/{id}", priv(a.revokeSession))
	mux.Handle("POST /v1/me/identities/{provider}", priv(a.link))
	mux.Handle("DELETE /v1/me/identities/{provider}", priv(a.unlink))

	mux.Handle("GET /v1/users", h(a.users))
	mux.Handle("GET /v1/users/by-handle/{handle}", h(a.userByHandle))
	mux.Handle("GET /v1/users/{id}", h(a.user))
	return httpx.Chain(mux, httpx.Timeout(15*time.Second))
}

type deviceIn struct {
	ID       string `json:"device_id"`
	Name     string `json:"device_name"`
	Platform string `json:"platform"`
}

func device(r *http.Request, d deviceIn) app.Device {
	return app.Device{ID: d.ID, Name: d.Name, Platform: d.Platform, IP: httpx.ClientIP(r), UserAgent: r.UserAgent()}
}

type signupIn struct {
	BirthDate   string `json:"birth_date,omitempty"` // YYYY-MM-DD; asked on first launch like TikTok
	DisplayName string `json:"display_name,omitempty"`
}

func (s signupIn) info() (app.NewUserInfo, error) {
	info := app.NewUserInfo{DisplayName: s.DisplayName}
	if s.BirthDate != "" {
		d, err := time.Parse("2006-01-02", s.BirthDate)
		if err != nil || d.After(time.Now()) || d.Year() < 1900 {
			return info, errs.BadRequest("birth_date_invalid", "Enter a valid birthday.")
		}
		info.BirthDate = &d
	}
	return info, nil
}

func (a *API) otpSend(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Channel string `json:"channel"`
		Target  string `json:"target"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	target, ttl, err := a.Svc.SendOTP(r.Context(), in.Channel, in.Target, httpx.ClientIP(r))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusAccepted, map[string]any{"target": target, "expires_in": int(ttl.Seconds())})
	return nil
}

func (a *API) otpVerify(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Channel string   `json:"channel"`
		Target  string   `json:"target"`
		Code    string   `json:"code"`
		Device  deviceIn `json:"device"`
		signupIn
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	info, err := in.signupIn.info()
	if err != nil {
		return err
	}
	res, err := a.Svc.VerifyOTP(r.Context(), in.Channel, in.Target, in.Code, device(r, in.Device), info)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, res)
	return nil
}

type oauthIn struct {
	providers.Credential
	Device deviceIn `json:"device"`
	signupIn
}

func (a *API) oauth(w http.ResponseWriter, r *http.Request) error {
	var in oauthIn
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	info, err := in.signupIn.info()
	if err != nil {
		return err
	}
	res, err := a.Svc.SignInWithProvider(r.Context(), r.PathValue("provider"), in.Credential, device(r, in.Device), info)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, res)
	return nil
}

func (a *API) passwordLogin(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Login    string   `json:"login"`
		Password string   `json:"password"`
		Device   deviceIn `json:"device"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	res, err := a.Svc.PasswordLogin(r.Context(), in.Login, in.Password, device(r, in.Device))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, res)
	return nil
}

func (a *API) refresh(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		RefreshToken string   `json:"refresh_token"`
		Device       deviceIn `json:"device"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	t, err := a.Svc.Refresh(r.Context(), in.RefreshToken, device(r, in.Device))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, t)
	return nil
}

func (a *API) logout(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.Logout(r.Context(), p); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) logoutAll(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		KeepCurrent bool `json:"keep_current"`
	}
	if r.ContentLength > 0 {
		if err := httpx.Decode(w, r, &in); err != nil {
			return err
		}
	}
	if err := a.Svc.LogoutAll(r.Context(), p, in.KeepCurrent); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) qrCreate(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Device deviceIn `json:"device"`
	}
	if r.ContentLength > 0 {
		if err := httpx.Decode(w, r, &in); err != nil {
			return err
		}
	}
	q, err := a.Svc.QRCreate(r.Context(), device(r, in.Device))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusCreated, q)
	return nil
}

func (a *API) qrPoll(w http.ResponseWriter, r *http.Request) error {
	status, res, err := a.Svc.QRPoll(r.Context(), r.PathValue("id"), r.URL.Query().Get("poll_token"),
		device(r, deviceIn{Name: "Web browser", Platform: "web"}))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"status": status, "result": res})
	return nil
}

func (a *API) qrInfo(w http.ResponseWriter, r *http.Request) error {
	info, err := a.Svc.QRInfo(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, info)
	return nil
}

func (a *API) qrApprove(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.QRApprove(r.Context(), p, r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) me(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	m, err := a.Svc.Me(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, m)
	return nil
}

func (a *API) updateMe(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in app.ProfileUpdate
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	m, err := a.Svc.UpdateProfile(r.Context(), p.UserID, in)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, m)
	return nil
}

func (a *API) deleteMe(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.DeleteAccount(r.Context(), p); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) setPassword(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		Current string `json:"current_password"`
		New     string `json:"new_password"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	if err := a.Svc.SetPassword(r.Context(), p, in.Current, in.New); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) sessions(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	list, err := a.Svc.Sessions(r.Context(), p)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": list})
	return nil
}

func (a *API) revokeSession(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.RevokeSessionByID(r.Context(), p, r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) link(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in providers.Credential
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	if err := a.Svc.LinkProvider(r.Context(), p.UserID, r.PathValue("provider"), in); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) unlink(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	if err := a.Svc.UnlinkProvider(r.Context(), p.UserID, r.PathValue("provider")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (a *API) user(w http.ResponseWriter, r *http.Request) error {
	u, err := a.Svc.PublicUser(r.Context(), r.PathValue("id"), false)
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "public, max-age=30")
	httpx.JSON(w, http.StatusOK, u)
	return nil
}

func (a *API) users(w http.ResponseWriter, r *http.Request) error {
	ids := strings.Split(r.URL.Query().Get("ids"), ",")
	list, err := a.Svc.PublicUsers(r.Context(), ids)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": list})
	return nil
}

func (a *API) userByHandle(w http.ResponseWriter, r *http.Request) error {
	u, err := a.Svc.PublicUser(r.Context(), r.PathValue("handle"), true)
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "public, max-age=30")
	httpx.JSON(w, http.StatusOK, u)
	return nil
}
