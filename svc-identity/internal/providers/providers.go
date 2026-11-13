// Package providers verifies third-party sign-in credentials and returns the
// external identity. Native SDKs on the phone do the sign-in UI; the app sends
// the resulting ID token / access token / auth code here, and the server
// verifies it directly with the provider. Never trust profile data the client
// sends without a token.
package providers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// External is a verified identity from a provider.
type External struct {
	Provider      string
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
	AvatarURL     string
}

// Credential is what the app sends for a provider sign-in.
type Credential struct {
	IDToken      string `json:"id_token,omitempty"`
	AccessToken  string `json:"access_token,omitempty"`
	Code         string `json:"code,omitempty"`
	CodeVerifier string `json:"code_verifier,omitempty"`
	RedirectURI  string `json:"redirect_uri,omitempty"`
	Nonce        string `json:"nonce,omitempty"`
	// FullName is sent by the app on the first Apple sign-in only (Apple puts
	// the name in the SDK response, not in the token).
	FullName string `json:"full_name,omitempty"`
}

var (
	ErrInvalid     = errors.New("providers: invalid credential")
	ErrUnsupported = errors.New("providers: provider not configured")
)

type Provider interface {
	Name() string
	Verify(ctx context.Context, c Credential) (External, error)
}

// Registry looks up configured providers by name.
type Registry map[string]Provider

func (r Registry) Get(name string) (Provider, error) {
	p, ok := r[name]
	if !ok {
		return nil, ErrUnsupported
	}
	return p, nil
}

// ---- OIDC (Google, Apple, LINE, Kakao, Facebook Limited Login) ----

type OIDC struct {
	name         string
	issuers      []string
	audiences    []string
	requireNonce bool
	verifier     *oidc.IDTokenVerifier
}

// NewOIDC builds a verifier from a JWKS URL. Keys are fetched lazily and
// cached; unknown key IDs trigger a refetch (provider key rotation).
func NewOIDC(name string, issuers, audiences []string, jwksURL string, requireNonce bool) *OIDC {
	ks := oidc.NewRemoteKeySet(context.Background(), jwksURL)
	v := oidc.NewVerifier("", ks, &oidc.Config{
		SkipClientIDCheck:    true, // checked below against several client IDs
		SkipIssuerCheck:      true, // checked below (Google uses two spellings)
		SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256},
	})
	return &OIDC{name: name, issuers: issuers, audiences: audiences, requireNonce: requireNonce, verifier: v}
}

func (p *OIDC) Name() string { return p.name }

// flexBool accepts true, "true" (Apple sends strings).
type flexBool bool

func (b *flexBool) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	v, _ := strconv.ParseBool(s)
	*b = flexBool(v)
	return nil
}

func (p *OIDC) Verify(ctx context.Context, c Credential) (External, error) {
	if c.IDToken == "" {
		return External{}, fmt.Errorf("%w: id_token required", ErrInvalid)
	}
	tok, err := p.verifier.Verify(ctx, c.IDToken)
	if err != nil {
		return External{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if !slices.Contains(p.issuers, tok.Issuer) {
		return External{}, fmt.Errorf("%w: issuer %q", ErrInvalid, tok.Issuer)
	}
	audOK := false
	for _, a := range tok.Audience {
		if slices.Contains(p.audiences, a) {
			audOK = true
			break
		}
	}
	if !audOK {
		return External{}, fmt.Errorf("%w: audience %v not allowed", ErrInvalid, tok.Audience)
	}
	if p.requireNonce || c.Nonce != "" {
		if c.Nonce == "" {
			return External{}, fmt.Errorf("%w: nonce required", ErrInvalid)
		}
		sum := sha256.Sum256([]byte(c.Nonce))
		if tok.Nonce != c.Nonce && tok.Nonce != hex.EncodeToString(sum[:]) {
			return External{}, fmt.Errorf("%w: nonce mismatch", ErrInvalid)
		}
	}
	var cl struct {
		Email         string   `json:"email"`
		EmailVerified flexBool `json:"email_verified"`
		Name          string   `json:"name"`
		Nickname      string   `json:"nickname"` // Kakao
		Picture       string   `json:"picture"`
	}
	if err := tok.Claims(&cl); err != nil {
		return External{}, fmt.Errorf("%w: claims: %v", ErrInvalid, err)
	}
	name := cl.Name
	if name == "" {
		name = cl.Nickname
	}
	if name == "" {
		name = c.FullName
	}
	return External{
		Provider: p.name, Subject: tok.Subject, Email: cl.Email,
		EmailVerified: bool(cl.EmailVerified), Name: name, AvatarURL: cl.Picture,
	}, nil
}

// ---- Facebook (access token from the Facebook SDK) ----

type Facebook struct {
	AppID, AppSecret, GraphURL string
	HTTP                       *http.Client
	limited                    *OIDC // iOS Limited Login sends an OIDC token instead
}

func NewFacebook(appID, appSecret, graphURL string, limited *OIDC) *Facebook {
	return &Facebook{AppID: appID, AppSecret: appSecret, GraphURL: strings.TrimRight(graphURL, "/"),
		HTTP: &http.Client{Timeout: 8 * time.Second}, limited: limited}
}

func (f *Facebook) Name() string { return "facebook" }

func (f *Facebook) Verify(ctx context.Context, c Credential) (External, error) {
	if c.IDToken != "" && f.limited != nil {
		ext, err := f.limited.Verify(ctx, c)
		ext.Provider = "facebook"
		return ext, err
	}
	if c.AccessToken == "" {
		return External{}, fmt.Errorf("%w: access_token required", ErrInvalid)
	}
	// 1. Confirm the token was issued to OUR app (prevents token substitution).
	var dbg struct {
		Data struct {
			AppID   string `json:"app_id"`
			IsValid bool   `json:"is_valid"`
			UserID  string `json:"user_id"`
		} `json:"data"`
	}
	q := "input_token=" + urlq(c.AccessToken) + "&access_token=" + urlq(f.AppID+"|"+f.AppSecret)
	if err := getJSON(ctx, f.HTTP, f.GraphURL+"/debug_token?"+q, "", &dbg); err != nil {
		return External{}, err
	}
	if !dbg.Data.IsValid || dbg.Data.AppID != f.AppID || dbg.Data.UserID == "" {
		return External{}, fmt.Errorf("%w: facebook token not valid for this app", ErrInvalid)
	}
	// 2. Fetch the profile, signed with appsecret_proof.
	var me struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Email   string `json:"email"`
		Picture struct {
			Data struct {
				URL string `json:"url"`
			} `json:"data"`
		} `json:"picture"`
	}
	proof := hmacHex(f.AppSecret, c.AccessToken)
	u := f.GraphURL + "/me?fields=id,name,email,picture.type(large)&access_token=" + urlq(c.AccessToken) + "&appsecret_proof=" + proof
	if err := getJSON(ctx, f.HTTP, u, "", &me); err != nil {
		return External{}, err
	}
	if me.ID != dbg.Data.UserID {
		return External{}, fmt.Errorf("%w: facebook user mismatch", ErrInvalid)
	}
	// Facebook only returns emails it has confirmed.
	return External{Provider: "facebook", Subject: me.ID, Email: me.Email, EmailVerified: me.Email != "",
		Name: me.Name, AvatarURL: me.Picture.Data.URL}, nil
}

// ---- X (OAuth 2.0 authorization code + PKCE) ----

type X struct {
	ClientID, ClientSecret string
	TokenURL, MeURL        string
	HTTP                   *http.Client
}

func NewX(clientID, clientSecret string) *X {
	return &X{ClientID: clientID, ClientSecret: clientSecret,
		TokenURL: "https://api.x.com/2/oauth2/token",
		MeURL:    "https://api.x.com/2/users/me?user.fields=profile_image_url,name,username",
		HTTP:     &http.Client{Timeout: 8 * time.Second}}
}

func (x *X) Name() string { return "x" }

func (x *X) Verify(ctx context.Context, c Credential) (External, error) {
	if c.Code == "" || c.CodeVerifier == "" || c.RedirectURI == "" {
		return External{}, fmt.Errorf("%w: code, code_verifier and redirect_uri required", ErrInvalid)
	}
	form := "grant_type=authorization_code&code=" + urlq(c.Code) + "&redirect_uri=" + urlq(c.RedirectURI) +
		"&code_verifier=" + urlq(c.CodeVerifier) + "&client_id=" + urlq(x.ClientID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, x.TokenURL, strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(x.ClientID, x.ClientSecret)
	resp, err := x.HTTP.Do(req)
	if err != nil {
		return External{}, fmt.Errorf("x token exchange: %w", err)
	}
	defer resp.Body.Close()
	var tok struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&tok)
	if resp.StatusCode != http.StatusOK || tok.AccessToken == "" {
		return External{}, fmt.Errorf("%w: x token exchange: %d %s", ErrInvalid, resp.StatusCode, tok.Error)
	}
	var me struct {
		Data struct {
			ID              string `json:"id"`
			Name            string `json:"name"`
			Username        string `json:"username"`
			ProfileImageURL string `json:"profile_image_url"`
		} `json:"data"`
	}
	if err := getJSON(ctx, x.HTTP, x.MeURL, tok.AccessToken, &me); err != nil {
		return External{}, err
	}
	if me.Data.ID == "" {
		return External{}, fmt.Errorf("%w: x user missing", ErrInvalid)
	}
	name := me.Data.Name
	if name == "" {
		name = me.Data.Username
	}
	return External{Provider: "x", Subject: me.Data.ID, Name: name, AvatarURL: me.Data.ProfileImageURL}, nil
}
