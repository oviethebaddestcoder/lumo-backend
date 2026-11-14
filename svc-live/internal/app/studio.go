package app

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// HostRoom is a past or current live session, for Creator Studio.
type HostRoom struct {
	ID          string     `json:"id"`
	Title       string     `json:"title"`
	Category    string     `json:"category"`
	Source      string     `json:"source"`
	Status      string     `json:"status"`
	PeakViewers int64      `json:"peak_viewers"`
	GiftCoins   int64      `json:"gift_coins"`
	SaveReplay  bool       `json:"save_replay"`
	StartedAt   time.Time  `json:"started_at"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
	DurationSec int64      `json:"duration_sec"`
}

// HostRooms lists a host's sessions, newest first. cursor = started_at (RFC3339Nano) of the last item.
func (s *Service) HostRooms(ctx context.Context, hostID, cursor string, limit int) ([]HostRoom, string, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	before := time.Now().Add(time.Hour)
	if t, err := time.Parse(time.RFC3339Nano, cursor); err == nil {
		before = t
	}
	rows, err := s.Pool.Query(ctx, `SELECT id::text, title, category, source, status, peak_viewers, gift_coins, save_replay, started_at, ended_at,
			EXTRACT(EPOCH FROM COALESCE(ended_at, now()) - started_at)::bigint
		FROM rooms WHERE host_id = $1 AND started_at < $2 ORDER BY started_at DESC LIMIT $3`, hostID, before, limit)
	if err != nil {
		return nil, "", err
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (HostRoom, error) {
		var h HostRoom
		return h, r.Scan(&h.ID, &h.Title, &h.Category, &h.Source, &h.Status, &h.PeakViewers, &h.GiftCoins, &h.SaveReplay,
			&h.StartedAt, &h.EndedAt, &h.DurationSec)
	})
	next := ""
	if len(list) == limit {
		next = list[len(list)-1].StartedAt.Format(time.RFC3339Nano)
	}
	return list, next, err
}

type HostSummary struct {
	Days        int   `json:"days"`
	Sessions    int64 `json:"sessions"`
	LiveSeconds int64 `json:"live_seconds"`
	AvgPeak     int64 `json:"avg_peak_viewers"`
	MaxPeak     int64 `json:"max_peak_viewers"`
	GiftCoins   int64 `json:"gift_coins"`
}

// HostSummaryFor aggregates a host's sessions over the last `days` days.
func (s *Service) HostSummaryFor(ctx context.Context, hostID string, days int) (HostSummary, error) {
	if days <= 0 || days > 90 {
		days = 7
	}
	h := HostSummary{Days: days}
	err := s.Pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(EXTRACT(EPOCH FROM COALESCE(ended_at, now()) - started_at)), 0)::bigint,
			COALESCE(avg(peak_viewers), 0)::bigint, COALESCE(max(peak_viewers), 0), COALESCE(sum(gift_coins), 0)
		FROM rooms WHERE host_id = $1 AND started_at > now() - make_interval(days => $2)`, hostID, days).
		Scan(&h.Sessions, &h.LiveSeconds, &h.AvgPeak, &h.MaxPeak, &h.GiftCoins)
	return h, err
}
