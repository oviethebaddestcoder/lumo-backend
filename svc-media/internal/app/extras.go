package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
)

// MinViewMS is how long a play must last to count as a view.
const MinViewMS = 1000

// ApplyViews is the batch consumer of feed.impressions: one UPDATE per video
// per batch, deduplicated per event so redelivery never double counts.
func (s *Service) ApplyViews(ctx context.Context, envs []events.Envelope) error {
	ids := make([]string, 0, len(envs))
	for _, e := range envs {
		ids = append(ids, e.ID)
	}
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `INSERT INTO processed_events (consumer, event_id) SELECT 'media-views', unnest($1::text[])
			ON CONFLICT DO NOTHING RETURNING event_id`, ids)
		if err != nil {
			return err
		}
		fresh, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		isFresh := make(map[string]bool, len(fresh))
		for _, id := range fresh {
			isFresh[id] = true
		}
		views := map[string]int64{}
		for _, e := range envs {
			if !isFresh[e.ID] || e.Type != events.Impression {
				continue
			}
			var d events.ImpressionData
			if e.Decode(&d) != nil || len(d.VideoID) != 36 || d.WatchMS < MinViewMS {
				continue
			}
			views[d.VideoID]++
		}
		keys := make([]string, 0, len(views))
		for k := range views {
			keys = append(keys, k)
		}
		slices.Sort(keys) // stable lock order across concurrent batches
		for _, k := range keys {
			if _, err := tx.Exec(ctx, `UPDATE videos SET views = views + $2 WHERE id = $1`, k, views[k]); err != nil {
				return err
			}
		}
		return nil
	})
}

// ByHashtag lists public ready videos tagged #tag, newest first.
func (s *Service) ByHashtag(ctx context.Context, tag, cursor string, limit int) ([]Video, string, error) {
	tag = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(tag), "#"))
	if tag == "" || len(tag) > 50 {
		return nil, "", errs.BadRequest("hashtag_invalid", "Give a hashtag.")
	}
	if limit <= 0 || limit > 50 {
		limit = 24
	}
	if cursor == "" {
		cursor = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+videoCols+` FROM videos
		WHERE hashtags @> ARRAY[$1]::text[] AND status = 'ready' AND visibility = 'public' AND id < $2
		ORDER BY id DESC LIMIT $3`, tag, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Video, error) { return scanVideo(r) })
	next := ""
	if len(list) == limit {
		next = list[len(list)-1].ID
	}
	return list, next, err
}

type ImageInput struct {
	Purpose     string `json:"purpose"` // avatar | thumbnail
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
}

type ImageUpload struct {
	UploadURL string `json:"upload_url"` // PUT the bytes here
	URL       string `json:"url"`        // public URL once uploaded
	ExpiresAt int64  `json:"expires_at"`
}

var imageExt = map[string]string{"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}

const maxImageBytes = 5 << 20

// CreateImageUpload hands out a short-lived presigned PUT for a profile photo
// (or thumbnail). Keys are random and namespaced by user, so a user can only
// ever publish under their own prefix.
func (s *Service) CreateImageUpload(ctx context.Context, p auth.Principal, in ImageInput) (ImageUpload, error) {
	if in.Purpose != "avatar" && in.Purpose != "thumbnail" {
		return ImageUpload{}, errs.BadRequest("purpose_invalid", "Purpose must be avatar or thumbnail.")
	}
	ext, ok := imageExt[in.ContentType]
	if !ok {
		return ImageUpload{}, errs.BadRequest("content_type_invalid", "Use a JPEG, PNG or WebP image.")
	}
	if in.SizeBytes <= 0 || in.SizeBytes > maxImageBytes {
		return ImageUpload{}, errs.BadRequest("too_large", "Images can be at most 5 MB.")
	}
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ImageUpload{}, err
	}
	key := "images/" + in.Purpose + "/" + p.UserID + "/" + hex.EncodeToString(b[:]) + "." + ext
	const ttl = 10 * time.Minute
	return ImageUpload{
		// Size and type are signed: S3 refuses a bigger file or another type.
		UploadURL: s.S3.PresignHeaders("PUT", key, nil, ttl, map[string]string{
			"Content-Length": strconv.FormatInt(in.SizeBytes, 10),
			"Content-Type":   in.ContentType,
		}),
		URL:       strings.TrimRight(s.CDNURL, "/") + "/" + key,
		ExpiresAt: time.Now().Add(ttl).Unix(),
	}, nil
}
