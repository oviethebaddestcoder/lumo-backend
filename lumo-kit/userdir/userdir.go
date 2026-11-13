// Package userdir looks up public user profiles from svc-identity with an
// in-process cache. Services use it to show names and avatars (chat, feed,
// live lists) without keeping a copy of 200M+ profiles themselves.
package userdir

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/cache"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/trace"
)

type Client struct {
	BaseURL string // e.g. http://svc-identity
	HTTP    *http.Client
	cache   *cache.Cache[events.UserPublic]
}

func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{Timeout: 2 * time.Second},
		cache: cache.New[events.UserPublic](5*time.Minute, 500_000)}
}

// Invalidate drops a cached profile (call on identity.user.updated).
func (c *Client) Invalidate(id string) { c.cache.Delete(id) }

// Get returns one profile.
func (c *Client) Get(ctx context.Context, id string) (events.UserPublic, error) {
	m, err := c.Many(ctx, []string{id})
	if err != nil {
		return events.UserPublic{}, err
	}
	u, ok := m[id]
	if !ok {
		return events.UserPublic{ID: id, DisplayName: "Lumo user"}, nil
	}
	return u, nil
}

// Many returns profiles for ids (missing users are absent from the map).
func (c *Client) Many(ctx context.Context, ids []string) (map[string]events.UserPublic, error) {
	out := make(map[string]events.UserPublic, len(ids))
	var miss []string
	for _, id := range ids {
		if u, ok := c.cache.Get(id); ok {
			out[id] = u
		} else if id != "" {
			miss = append(miss, id)
		}
	}
	for len(miss) > 0 {
		n := min(100, len(miss))
		batch := miss[:n]
		miss = miss[n:]
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/users?ids="+url.QueryEscape(strings.Join(batch, ",")), nil)
		if tp := trace.Traceparent(ctx); tp != "" {
			req.Header.Set(trace.Header, tp)
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return out, fmt.Errorf("userdir: %w", err)
		}
		var body struct {
			Items []events.UserPublic `json:"items"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			return out, fmt.Errorf("userdir: status %d: %v", resp.StatusCode, err)
		}
		for _, u := range body.Items {
			c.cache.Set(u.ID, u)
			out[u.ID] = u
		}
	}
	return out, nil
}
