// Package api is svc-wallet's HTTP layer.
package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/stores"
)

type API struct {
	Svc      *app.Service
	Verifier *auth.Verifier
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	priv := func(f httpx.Handler) http.Handler { return a.Verifier.Middleware(true, false)(f) }
	pub := func(f httpx.Handler) http.Handler { return f }

	mux.Handle("GET /v1/wallet/catalog", pub(a.catalog))
	mux.Handle("GET /v1/wallet", priv(a.balance))
	mux.Handle("GET /v1/wallet/history", priv(a.history))
	mux.Handle("GET /v1/wallet/purchase-tag", priv(a.purchaseTag))
	mux.Handle("POST /v1/wallet/gifts/send", priv(a.sendGift))
	mux.Handle("POST /v1/wallet/purchases/google", priv(a.google))
	mux.Handle("POST /v1/wallet/purchases/apple", priv(a.apple))
	mux.Handle("POST /v1/wallet/web/checkout", priv(a.webCheckout))
	mux.Handle("POST /v1/wallet/payouts", priv(a.payout))
	mux.Handle("GET /v1/wallet/payouts", priv(a.myPayouts))
	mux.Handle("GET /v1/wallet/stats", priv(a.stats))
	mux.Handle("GET /v1/wallet/earnings", priv(a.earnings))
	mux.Handle("GET /v1/wallet/levels", pub(a.levels))
	mux.Handle("GET /v1/wallet/backpack", priv(a.backpack))
	mux.Handle("POST /v1/wallet/checkin", priv(a.checkin))
	mux.Handle("GET /v1/wallet/banks", priv(a.banks))
	mux.Handle("GET /v1/wallet/payout-account", priv(a.payoutAccount))
	mux.Handle("PUT /v1/wallet/payout-account", priv(a.setPayoutAccount))
	mux.Handle("GET /v1/wallet/admin/payouts", priv(a.adminPayouts))
	mux.Handle("POST /v1/wallet/admin/payouts/{id}/approve", priv(a.adminApprove))
	mux.Handle("POST /v1/wallet/admin/payouts/{id}/reject", priv(a.adminReject))
	// Webhooks authenticate by signature / re-verification, not by user token.
	mux.Handle("POST /v1/wallet/webhooks/paystack", pub(a.paystackHook))
	mux.Handle("POST /v1/wallet/webhooks/apple", pub(a.appleHook))
	return httpx.Chain(mux, httpx.Timeout(20*time.Second))
}

func (a *API) catalog(w http.ResponseWriter, r *http.Request) error {
	gifts, packs, err := a.Svc.Catalog(r.Context())
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
	httpx.JSON(w, http.StatusOK, map[string]any{"gifts": gifts, "packs": packs})
	return nil
}

func (a *API) balance(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	b, err := a.Svc.Balance(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, b)
	return nil
}

func (a *API) history(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	before, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := a.Svc.History(r.Context(), p.UserID, before, limit)
	if err != nil {
		return err
	}
	page := httpx.Page[app.Entry]{Items: items}
	if len(items) > 0 {
		page.NextCursor = strconv.FormatInt(items[len(items)-1].ID, 10)
	}
	httpx.JSON(w, http.StatusOK, page)
	return nil
}

func (a *API) purchaseTag(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	httpx.JSON(w, http.StatusOK, map[string]string{
		"google_obfuscated_account_id": a.Svc.AccountTag(p.UserID),
		"apple_app_account_token":      p.UserID,
	})
	return nil
}

func (a *API) sendGift(w http.ResponseWriter, r *http.Request) error {
	p, err := auth.Adult(r)
	if err != nil {
		return err
	}
	var in app.SendGiftInput
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	in.SenderID, in.RequestID = p.UserID, r.Header.Get("Idempotency-Key")
	rec, err := a.Svc.SendGift(r.Context(), in)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, rec)
	return nil
}

func (a *API) google(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		ProductID     string `json:"product_id"`
		PurchaseToken string `json:"purchase_token"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	res, err := a.Svc.VerifyGoogle(r.Context(), p.UserID, in.ProductID, in.PurchaseToken)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, res)
	return nil
}

func (a *API) apple(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		TransactionID string `json:"transaction_id"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	res, err := a.Svc.VerifyApple(r.Context(), p.UserID, in.TransactionID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, res)
	return nil
}

func (a *API) webCheckout(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		PackID      string `json:"pack_id"`
		Email       string `json:"email"`
		CallbackURL string `json:"callback_url"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	u, ref, err := a.Svc.WebCheckout(r.Context(), p.UserID, in.Email, in.PackID, in.CallbackURL)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]string{"authorization_url": u, "reference": ref})
	return nil
}

func (a *API) payout(w http.ResponseWriter, r *http.Request) error {
	p, err := auth.Adult(r)
	if err != nil {
		return err
	}
	var in struct {
		Diamonds int64 `json:"diamonds"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	res, err := a.Svc.RequestPayout(r.Context(), p.UserID, r.Header.Get("Idempotency-Key"), in.Diamonds)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusAccepted, res)
	return nil
}

func (a *API) paystackHook(w http.ResponseWriter, r *http.Request) error {
	if a.Svc.Paystack == nil {
		return errs.ErrNotFound
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if !a.Svc.Paystack.ValidWebhook(body, r.Header.Get("X-Paystack-Signature")) {
		return errs.Unauthorized("bad_signature", "Invalid signature.")
	}
	var ev struct {
		Event string `json:"event"`
		Data  struct {
			Reference string `json:"reference"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		return errs.BadRequest("invalid_json", "Bad payload.")
	}
	switch ev.Event {
	case "charge.success":
		if err := a.Svc.PaystackCharge(r.Context(), ev.Data.Reference); err != nil {
			return err // non-2xx makes Paystack retry
		}
	case "transfer.success", "transfer.failed", "transfer.reversed":
		if err := a.Svc.PaystackTransfer(r.Context(), ev.Event, ev.Data.Reference); err != nil {
			return err
		}
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (a *API) appleHook(w http.ResponseWriter, r *http.Request) error {
	if a.Svc.Apple == nil {
		return errs.ErrNotFound
	}
	var in struct {
		SignedPayload string `json:"signedPayload"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	typ, txID, err := stores.NotificationTransactionID(in.SignedPayload)
	if err != nil {
		return errs.BadRequest("invalid_payload", "Bad notification.")
	}
	if typ == "REFUND" || typ == "REVOKE" {
		// Never trust the notification body: confirm with Apple first.
		t, _, err := a.Svc.Apple.Transaction(r.Context(), txID)
		if err != nil {
			return err
		}
		if t.RevocationDate != 0 {
			if err := a.Svc.Reverse(r.Context(), "app_store", t.TransactionID, "apple_"+typ); err != nil {
				return err
			}
		}
	}
	slog.InfoContext(r.Context(), "apple notification", "type", typ)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (a *API) stats(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	st, err := a.Svc.Stats(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, st)
	return nil
}

func (a *API) levels(w http.ResponseWriter, r *http.Request) error {
	list, err := a.Svc.Levels(r.Context(), strings.Split(r.URL.Query().Get("ids"), ","))
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
	httpx.JSON(w, http.StatusOK, map[string]any{"items": list})
	return nil
}

func (a *API) backpack(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	bp, err := a.Svc.Backpack(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, bp)
	return nil
}

func (a *API) checkin(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	res, err := a.Svc.Checkin(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, res)
	return nil
}

func (a *API) banks(w http.ResponseWriter, r *http.Request) error {
	list, err := a.Svc.Banks(r.Context())
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": list})
	return nil
}

func (a *API) payoutAccount(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	pa, err := a.Svc.PayoutAccountOf(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	// The rate and minimum let apps show "≈ ₦…" before the creator asks;
	// finance still sees and approves the exact amount.
	httpx.JSON(w, http.StatusOK, map[string]any{"account": pa, "kobo_per_diamond": a.Svc.KoboPerDiamond, "min_diamonds": a.Svc.MinPayout})
	return nil
}

func (a *API) setPayoutAccount(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	var in struct {
		BankCode      string `json:"bank_code"`
		AccountNumber string `json:"account_number"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	pa, err := a.Svc.SetPayoutAccount(r.Context(), p.UserID, in.BankCode, in.AccountNumber)
	if err != nil {
		return err
	}
	// The rate and minimum let apps show "≈ ₦…" before the creator asks;
	// finance still sees and approves the exact amount.
	httpx.JSON(w, http.StatusOK, map[string]any{"account": pa, "kobo_per_diamond": a.Svc.KoboPerDiamond, "min_diamonds": a.Svc.MinPayout})
	return nil
}

func (a *API) myPayouts(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	list, err := a.Svc.MyPayouts(r.Context(), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": list})
	return nil
}

// Finance console. Requires the "finance" role in the access token.
func (a *API) adminPayouts(w http.ResponseWriter, r *http.Request) error {
	if _, err := auth.Role(r, "finance"); err != nil {
		return err
	}
	status := r.URL.Query().Get("status")
	if status == "" {
		status = "pending_review"
	}
	list, err := a.Svc.AdminPayouts(r.Context(), status, 0)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": list})
	return nil
}

func (a *API) adminApprove(w http.ResponseWriter, r *http.Request) error {
	p, err := auth.Role(r, "finance")
	if err != nil {
		return err
	}
	po, err := a.Svc.ApprovePayout(r.Context(), r.PathValue("id"), p.UserID)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, po)
	return nil
}

func (a *API) adminReject(w http.ResponseWriter, r *http.Request) error {
	p, err := auth.Role(r, "finance")
	if err != nil {
		return err
	}
	var in struct {
		Reason string `json:"reason"`
	}
	if err := httpx.Decode(w, r, &in); err != nil {
		return err
	}
	po, err := a.Svc.RejectPayout(r.Context(), r.PathValue("id"), p.UserID, in.Reason)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, po)
	return nil
}

func (a *API) earnings(w http.ResponseWriter, r *http.Request) error {
	p, _ := auth.User(r)
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	list, err := a.Svc.Earnings(r.Context(), p.UserID, days)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": list})
	return nil
}
