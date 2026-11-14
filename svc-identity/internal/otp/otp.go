// Package otp issues and checks one-time sign-in codes for phone and email.
//
// Codes are 6 digits, stored in Redis only as an HMAC (never in plain text),
// expire after 5 minutes and allow 5 guesses. Sending is rate limited per
// target and per IP so the SMS bill can't be run up by a script.
package otp

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/mail"
	"strings"
	"time"

	"github.com/nyaruka/phonenumbers"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/redisx"
	"github.com/redis/go-redis/v9"
)

const (
	ChannelSMS   = "sms"
	ChannelEmail = "email"

	codeTTL     = 5 * time.Minute
	maxAttempts = 5
)

var (
	ErrInvalidTarget = errs.BadRequest("invalid_target", "Enter a valid phone number or email.")
	ErrCodeInvalid   = errs.BadRequest("code_invalid", "That code is wrong or has expired.")
	ErrTooManyTries  = errs.TooMany("code_attempts", "Too many wrong codes. Request a new one.")
)

// Sender delivers a code. Implementations must not log the code in production.
type Sender interface {
	Send(ctx context.Context, to, code string) error
}

type Service struct {
	R         redis.UniversalClient
	Limiter   redisx.Limiter
	Secret    []byte
	Region    string
	SMS       Sender
	Email     Sender
	TestCodes map[string]string // target -> fixed code (store reviewers)
	// PerIPHour caps codes sent per client IP per hour. Mobile carriers put
	// thousands of people behind one address (CGNAT), so this is a guard
	// against SMS-pumping floods, not a per-person limit; per-target limits
	// do that. Default 200.
	PerIPHour int
}

// Normalize validates and canonicalizes a target: E.164 for phones,
// lower-case for email.
func (s *Service) Normalize(channel, target string) (string, error) {
	target = strings.TrimSpace(target)
	switch channel {
	case ChannelSMS:
		num, err := phonenumbers.Parse(target, s.Region)
		if err != nil || !phonenumbers.IsValidNumber(num) {
			return "", ErrInvalidTarget
		}
		return phonenumbers.Format(num, phonenumbers.E164), nil
	case ChannelEmail:
		addr, err := mail.ParseAddress(target)
		if err != nil || addr.Address != target || !strings.Contains(target, ".") || len(target) > 254 {
			return "", ErrInvalidTarget
		}
		return strings.ToLower(addr.Address), nil
	default:
		return "", errs.BadRequest("invalid_channel", "Channel must be sms or email.")
	}
}

func key(channel, target string) string { return "otp:" + channel + ":" + target }

func (s *Service) mac(target, code string) string {
	m := hmac.New(sha256.New, s.Secret)
	m.Write([]byte(target + "|" + code))
	return hex.EncodeToString(m.Sum(nil))
}

// Send creates a code for target (already normalized) and delivers it.
func (s *Service) Send(ctx context.Context, channel, target, ip string) (time.Duration, error) {
	for _, l := range []struct {
		k     string
		limit int
		win   time.Duration
	}{
		{"otp:t1:" + target, 1, time.Minute},
		{"otp:t60:" + target, 5, time.Hour},
		{"otp:ip:" + ip, cmp.Or(s.PerIPHour, 200), time.Hour},
	} {
		res, err := s.Limiter.Allow(ctx, l.k, l.limit, l.win)
		if err != nil {
			return 0, errs.Unavailable("otp_unavailable", "Can't send codes right now. Try again.").Wrap(err)
		}
		if !res.Allowed {
			otpSent.WithLabelValues(channel, "rate_limited").Inc()
			return 0, errs.ErrRateLimited.With("retry_after_seconds", int(res.RetryAfter.Seconds())+1)
		}
	}

	code, test := s.TestCodes[target]
	if !test {
		n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
		if err != nil {
			return 0, err
		}
		code = fmt.Sprintf("%06d", n.Int64())
	}
	k := key(channel, target)
	pipe := s.R.TxPipeline()
	pipe.HSet(ctx, k, "mac", s.mac(target, code), "attempts", 0)
	pipe.PExpire(ctx, k, codeTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("otp: store: %w", err)
	}
	if test {
		otpSent.WithLabelValues(channel, "test_account").Inc()
		return codeTTL, nil
	}
	sender := s.SMS
	if channel == ChannelEmail {
		sender = s.Email
	}
	if err := sender.Send(ctx, target, code); err != nil {
		s.R.Del(context.WithoutCancel(ctx), k)
		otpSent.WithLabelValues(channel, "failed").Inc()
		return 0, errs.Unavailable("otp_send_failed", "We couldn't send the code. Try again.").Wrap(err)
	}
	otpSent.WithLabelValues(channel, "sent").Inc()
	return codeTTL, nil
}

// otpSent counts code sends. SMS costs money per message, so a sudden rise
// is the signature of SMS pumping fraud (alert OTPSendSpike).
var otpSent = metrics.NewCounterVec("identity_otp_sent_total", "Sign-in codes by channel and result (sent, failed, rate_limited, test_account).", "channel", "result")

// checkScript atomically counts an attempt and returns the stored MAC.
var checkScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return {-1, ''} end
local a = redis.call('HINCRBY', KEYS[1], 'attempts', 1)
return {a, redis.call('HGET', KEYS[1], 'mac')}
`)

// Verify consumes the code if it matches.
func (s *Service) Verify(ctx context.Context, channel, target, code string) error {
	if len(code) != 6 {
		return ErrCodeInvalid
	}
	k := key(channel, target)
	res, err := checkScript.Run(ctx, s.R, []string{k}).Slice()
	if err != nil {
		return fmt.Errorf("otp: check: %w", err)
	}
	attempts, _ := res[0].(int64)
	stored, _ := res[1].(string)
	if attempts < 0 {
		return ErrCodeInvalid
	}
	if attempts > maxAttempts {
		s.R.Del(ctx, k)
		return ErrTooManyTries
	}
	if !hmac.Equal([]byte(stored), []byte(s.mac(target, code))) {
		return ErrCodeInvalid
	}
	if n, err := s.R.Del(ctx, k).Result(); err != nil || n == 0 {
		return errors.Join(ErrCodeInvalid, err) // used concurrently by another request
	}
	return nil
}
