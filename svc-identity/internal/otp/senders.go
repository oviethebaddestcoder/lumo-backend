package otp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// LogSender prints codes to the log. Development only.
type LogSender struct{ Channel string }

func (l LogSender) Send(ctx context.Context, to, code string) error {
	slog.WarnContext(ctx, "DEV OTP (never enable in production)", "channel", l.Channel, "to", to, "code", code)
	return nil
}

// Termii sends SMS through Termii (Nigerian provider with DND-route delivery).
type Termii struct {
	APIKey, SenderID string
	BaseURL          string
}

func (t Termii) Send(ctx context.Context, to, code string) error {
	base := t.BaseURL
	if base == "" {
		base = "https://api.ng.termii.com"
	}
	body, _ := json.Marshal(map[string]string{
		"to":      strings.TrimPrefix(to, "+"),
		"from":    t.SenderID,
		"sms":     fmt.Sprintf("Your Lumo code is %s. It expires in 5 minutes. Don't share it.", code),
		"type":    "plain",
		"channel": "dnd",
		"api_key": t.APIKey,
	})
	return post(ctx, base+"/api/sms/send", "application/json", body, nil)
}

// Twilio sends SMS through Twilio's Messages API (international fallback).
type Twilio struct {
	AccountSID, AuthToken, From string
}

func (t Twilio) Send(ctx context.Context, to, code string) error {
	form := url.Values{"To": {to}, "From": {t.From}, "Body": {fmt.Sprintf("Your Lumo code is %s. It expires in 5 minutes.", code)}}
	u := "https://api.twilio.com/2010-04-01/Accounts/" + t.AccountSID + "/Messages.json"
	return post(ctx, u, "application/x-www-form-urlencoded", []byte(form.Encode()), func(r *http.Request) {
		r.SetBasicAuth(t.AccountSID, t.AuthToken)
	})
}

// Resend sends email codes through Resend.
type Resend struct {
	APIKey, From string
}

func (r Resend) Send(ctx context.Context, to, code string) error {
	body, _ := json.Marshal(map[string]any{
		"from":    r.From,
		"to":      []string{to},
		"subject": code + " is your Lumo code",
		"text":    fmt.Sprintf("Your Lumo sign-in code is %s.\n\nIt expires in 5 minutes. If you didn't ask for it, ignore this email.", code),
	})
	return post(ctx, "https://api.resend.com/emails", "application/json", body, func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+r.APIKey)
	})
}

func post(ctx context.Context, u, ct string, body []byte, mod func(*http.Request)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", ct)
	if mod != nil {
		mod(req)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("send: status %d: %s", resp.StatusCode, b)
	}
	return nil
}
