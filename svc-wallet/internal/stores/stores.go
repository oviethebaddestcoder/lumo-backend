// Package stores verifies purchases with Google Play, the App Store and
// Paystack. Each verifier talks to the store's server API directly; the
// receipt the app sends is only a pointer, never trusted on its own.
package stores

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/gauth"
)

var (
	ErrInvalid  = errors.New("stores: purchase not valid")
	ErrNotOwner = errors.New("stores: purchase belongs to another account")
)

// Verified is a confirmed, paid, unrevoked purchase.
type Verified struct {
	Channel   string
	ProductID string
	StoreRef  string
	Raw       json.RawMessage
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

// ---- Google Play ----

type GooglePlay struct {
	Package string
	Tokens  *gauth.TokenSource // scope androidpublisher
	BaseURL string
}

func (g *GooglePlay) api() string {
	if g.BaseURL != "" {
		return g.BaseURL
	}
	return "https://androidpublisher.googleapis.com/androidpublisher/v3/applications/" + url.PathEscape(g.Package)
}

// Verify checks a one-time product purchase. accountTag must equal the
// obfuscatedAccountId the app set in BillingFlowParams for this user, which
// stops one user from redeeming another user's purchase token.
func (g *GooglePlay) Verify(ctx context.Context, productID, token, accountTag string) (Verified, error) {
	var p struct {
		PurchaseState               int    `json:"purchaseState"` // 0 purchased, 1 canceled, 2 pending
		ConsumptionState            int    `json:"consumptionState"`
		OrderID                     string `json:"orderId"`
		ObfuscatedExternalAccountID string `json:"obfuscatedExternalAccountId"`
	}
	u := g.api() + "/purchases/products/" + url.PathEscape(productID) + "/tokens/" + url.PathEscape(token)
	raw, err := g.call(ctx, http.MethodGet, u, &p)
	if err != nil {
		return Verified{}, err
	}
	if p.PurchaseState != 0 {
		return Verified{}, fmt.Errorf("%w: purchase state %d", ErrInvalid, p.PurchaseState)
	}
	if p.ObfuscatedExternalAccountID != accountTag {
		return Verified{}, ErrNotOwner
	}
	return Verified{Channel: "google_play", ProductID: productID, StoreRef: token, Raw: raw}, nil
}

// Consume marks the purchase consumed so the user can buy the pack again.
// Google refunds consumables that are not acknowledged/consumed in 3 days.
func (g *GooglePlay) Consume(ctx context.Context, productID, token string) error {
	u := g.api() + "/purchases/products/" + url.PathEscape(productID) + "/tokens/" + url.PathEscape(token) + ":consume"
	_, err := g.call(ctx, http.MethodPost, u, nil)
	return err
}

// Voided lists purchases refunded or charged back since `since`.
func (g *GooglePlay) Voided(ctx context.Context, since time.Time) ([]string, error) {
	var out struct {
		VoidedPurchases []struct {
			PurchaseToken string `json:"purchaseToken"`
		} `json:"voidedPurchases"`
	}
	u := g.api() + "/purchases/voidedpurchases?startTime=" + fmt.Sprint(since.UnixMilli()) + "&maxResults=1000"
	if _, err := g.call(ctx, http.MethodGet, u, &out); err != nil {
		return nil, err
	}
	toks := make([]string, 0, len(out.VoidedPurchases))
	for _, v := range out.VoidedPurchases {
		toks = append(toks, v.PurchaseToken)
	}
	return toks, nil
}

func (g *GooglePlay) call(ctx context.Context, method, u string, v any) (json.RawMessage, error) {
	tok, err := g.Tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequestWithContext(ctx, method, u, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusGone {
		return nil, fmt.Errorf("%w: google %d %s", ErrInvalid, resp.StatusCode, truncate(body))
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("google play: %d %s", resp.StatusCode, truncate(body))
	}
	if v != nil && len(body) > 0 {
		if err := json.Unmarshal(body, v); err != nil {
			return nil, err
		}
	}
	return body, nil
}

// ---- App Store (App Store Server API) ----

type AppStore struct {
	IssuerID string
	KeyID    string
	Key      *ecdsa.PrivateKey // .p8 key from App Store Connect
	BundleID string
	Sandbox  bool
}

func ParseAppleKey(p8 []byte) (*ecdsa.PrivateKey, error) {
	return jwt.ParseECPrivateKeyFromPEM(p8)
}

func (a *AppStore) host() string {
	if a.Sandbox {
		return "https://api.storekit-sandbox.itunes.apple.com"
	}
	return "https://api.storekit.itunes.apple.com"
}

func (a *AppStore) bearer() (string, error) {
	now := time.Now()
	t := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": a.IssuerID, "iat": now.Unix(), "exp": now.Add(15 * time.Minute).Unix(),
		"aud": "appstoreconnect-v1", "bid": a.BundleID,
	})
	t.Header["kid"] = a.KeyID
	return t.SignedString(a.Key)
}

// AppleTransaction is the decoded signedTransactionInfo payload.
type AppleTransaction struct {
	TransactionID         string `json:"transactionId"`
	OriginalTransactionID string `json:"originalTransactionId"`
	BundleID              string `json:"bundleId"`
	ProductID             string `json:"productId"`
	Type                  string `json:"type"`
	AppAccountToken       string `json:"appAccountToken"`
	RevocationDate        int64  `json:"revocationDate"`
}

// Transaction fetches a transaction from Apple. The JWS comes straight from
// Apple's API over TLS with our credentials, so its payload is trusted
// without re-verifying the certificate chain.
func (a *AppStore) Transaction(ctx context.Context, txID string) (AppleTransaction, json.RawMessage, error) {
	b, err := a.bearer()
	if err != nil {
		return AppleTransaction{}, nil, err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, a.host()+"/inApps/v1/transactions/"+url.PathEscape(txID), nil)
	req.Header.Set("Authorization", "Bearer "+b)
	resp, err := httpClient.Do(req)
	if err != nil {
		return AppleTransaction{}, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest {
		return AppleTransaction{}, nil, fmt.Errorf("%w: apple %d", ErrInvalid, resp.StatusCode)
	}
	if resp.StatusCode/100 != 2 {
		return AppleTransaction{}, nil, fmt.Errorf("app store: %d %s", resp.StatusCode, truncate(body))
	}
	var out struct {
		SignedTransactionInfo string `json:"signedTransactionInfo"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return AppleTransaction{}, nil, err
	}
	payload, err := jwsPayload(out.SignedTransactionInfo)
	if err != nil {
		return AppleTransaction{}, nil, err
	}
	var t AppleTransaction
	if err := json.Unmarshal(payload, &t); err != nil {
		return AppleTransaction{}, nil, err
	}
	return t, payload, nil
}

// Verify checks a consumable purchase. appAccountToken must be the user's ID
// (the app sets it when starting the purchase).
func (a *AppStore) Verify(ctx context.Context, txID, userID string) (Verified, error) {
	t, raw, err := a.Transaction(ctx, txID)
	if err != nil {
		return Verified{}, err
	}
	if t.BundleID != a.BundleID || t.Type != "Consumable" || t.RevocationDate != 0 {
		return Verified{}, fmt.Errorf("%w: bundle %s type %s revoked %d", ErrInvalid, t.BundleID, t.Type, t.RevocationDate)
	}
	if !strings.EqualFold(t.AppAccountToken, userID) {
		return Verified{}, ErrNotOwner
	}
	return Verified{Channel: "app_store", ProductID: t.ProductID, StoreRef: t.TransactionID, Raw: raw}, nil
}

// NotificationTransactionID extracts the transaction ID from an App Store
// Server Notification V2. The caller must re-fetch the transaction from
// Apple before acting (the notification itself is not trusted).
func NotificationTransactionID(signedPayload string) (notificationType, txID string, err error) {
	p, err := jwsPayload(signedPayload)
	if err != nil {
		return "", "", err
	}
	var n struct {
		NotificationType string `json:"notificationType"`
		Data             struct {
			SignedTransactionInfo string `json:"signedTransactionInfo"`
		} `json:"data"`
	}
	if err := json.Unmarshal(p, &n); err != nil {
		return "", "", err
	}
	tp, err := jwsPayload(n.Data.SignedTransactionInfo)
	if err != nil {
		return n.NotificationType, "", err
	}
	var t AppleTransaction
	err = json.Unmarshal(tp, &t)
	return n.NotificationType, t.TransactionID, err
}

func jwsPayload(jws string) ([]byte, error) {
	parts := strings.Split(jws, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: malformed JWS", ErrInvalid)
	}
	return base64.RawURLEncoding.DecodeString(parts[1])
}

// ---- Paystack (web wallet: card, bank transfer, USSD) ----

type Paystack struct {
	SecretKey string
	BaseURL   string
}

func (p *Paystack) base() string {
	if p.BaseURL != "" {
		return p.BaseURL
	}
	return "https://api.paystack.co"
}

// Initialize starts a checkout and returns the hosted payment URL.
func (p *Paystack) Initialize(ctx context.Context, email, reference string, amountKobo int64, meta map[string]string, callbackURL string) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"email": email, "amount": amountKobo, "currency": "NGN", "reference": reference,
		"metadata": meta, "callback_url": callbackURL,
		"channels": []string{"card", "bank", "ussd", "bank_transfer"},
	})
	var out struct {
		Status bool `json:"status"`
		Data   struct {
			AuthorizationURL string `json:"authorization_url"`
		} `json:"data"`
		Message string `json:"message"`
	}
	if err := p.do(ctx, http.MethodPost, "/transaction/initialize", body, &out); err != nil {
		return "", err
	}
	if !out.Status {
		return "", fmt.Errorf("paystack: %s", out.Message)
	}
	return out.Data.AuthorizationURL, nil
}

// PaystackTx is a verified Paystack transaction.
type PaystackTx struct {
	Status    string         `json:"status"`
	Reference string         `json:"reference"`
	Amount    int64          `json:"amount"`
	Currency  string         `json:"currency"`
	Metadata  map[string]any `json:"metadata"`
}

// VerifyTx asks Paystack for the authoritative status of a reference.
func (p *Paystack) VerifyTx(ctx context.Context, reference string) (PaystackTx, json.RawMessage, error) {
	var out struct {
		Status bool            `json:"status"`
		Data   json.RawMessage `json:"data"`
	}
	if err := p.do(ctx, http.MethodGet, "/transaction/verify/"+url.PathEscape(reference), nil, &out); err != nil {
		return PaystackTx{}, nil, err
	}
	var t PaystackTx
	if err := json.Unmarshal(out.Data, &t); err != nil {
		return t, nil, err
	}
	return t, out.Data, nil
}

// ValidWebhook checks the x-paystack-signature header (HMAC-SHA512 of the raw body).
func (p *Paystack) ValidWebhook(body []byte, signature string) bool {
	m := hmac.New(sha512.New, []byte(p.SecretKey))
	m.Write(body)
	want := hex.EncodeToString(m.Sum(nil))
	return hmac.Equal([]byte(want), []byte(strings.ToLower(signature)))
}

func (p *Paystack) do(ctx context.Context, method, path string, body []byte, v any) error {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, _ := http.NewRequestWithContext(ctx, method, p.base()+path, r)
	req.Header.Set("Authorization", "Bearer "+p.SecretKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("paystack %s: %d %s", path, resp.StatusCode, truncate(b))
	}
	return json.Unmarshal(b, v)
}

func truncate(b []byte) string {
	if len(b) > 300 {
		b = b[:300]
	}
	return string(b)
}
