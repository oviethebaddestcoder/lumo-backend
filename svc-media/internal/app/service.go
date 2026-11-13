// Package app is svc-media: uploads, video records and their lifecycle.
//
// Upload flow (bytes never pass through our servers):
//  1. POST /v1/media/uploads -> video row + S3 multipart upload + presigned part URLs.
//  2. The app PUTs each 16 MiB part straight to object storage (resumable:
//     a failed part is retried alone).
//  3. POST /v1/media/uploads/{id}/complete -> media.jobs event.
//  4. worker-transcoder builds the HLS ladder and reports on media.job_results.
//  5. The video turns ready; media.videos (compacted) carries its full state
//     to feed, social and anyone else who needs it.
package app

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/errs"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/outbox"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/s3x"
)

const (
	producer = "svc-media"
	partSize = 16 << 20
)

var maxSize = map[string]int64{"short": 300 << 20, "long": 10 << 30}

type Service struct {
	Pool   *pgxpool.Pool
	S3     *s3x.Client
	CDNURL string // public base URL for HLS output and thumbnails
}

type Video struct {
	ID           string     `json:"id"`
	OwnerID      string     `json:"owner_id"`
	Kind         string     `json:"kind"`
	Status       string     `json:"status"`
	Visibility   string     `json:"visibility"`
	Title        string     `json:"title"`
	Description  string     `json:"description"`
	Hashtags     []string   `json:"hashtags"`
	HLSURL       string     `json:"hls_url,omitempty"`
	ThumbnailURL string     `json:"thumbnail_url,omitempty"`
	DurationMS   int64      `json:"duration_ms"`
	Width        int        `json:"width"`
	Height       int        `json:"height"`
	CreatedAt    time.Time  `json:"created_at"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
	sourceKey    string
}

const videoCols = `id::text, owner_id::text, kind, status, visibility, title, description, hashtags, hls_url, thumbnail_url,
	duration_ms, width, height, created_at, published_at, source_key`

func scanVideo(r pgx.Row) (Video, error) {
	var v Video
	err := r.Scan(&v.ID, &v.OwnerID, &v.Kind, &v.Status, &v.Visibility, &v.Title, &v.Description, &v.Hashtags, &v.HLSURL,
		&v.ThumbnailURL, &v.DurationMS, &v.Width, &v.Height, &v.CreatedAt, &v.PublishedAt, &v.sourceKey)
	if db.IsNoRows(err) {
		return v, errs.ErrNotFound
	}
	return v, err
}

func (v Video) state() events.Envelope {
	return events.MustNew(producer, events.VideoState, v.ID, events.VideoStateData{
		ID: v.ID, OwnerID: v.OwnerID, Kind: v.Kind, Status: v.Status, Visibility: v.Visibility, Title: v.Title,
		Description: v.Description, Hashtags: v.Hashtags, DurationMS: v.DurationMS, Width: v.Width, Height: v.Height,
		HLSURL: v.HLSURL, ThumbnailURL: v.ThumbnailURL, CreatedAt: v.CreatedAt, PublishedAt: v.PublishedAt,
	})
}

// emit writes the compacted state snapshot plus a lifecycle event.
func emit(ctx context.Context, tx db.Querier, v Video, lifecycle string) error {
	if err := outbox.Insert(ctx, tx, events.TopicMediaVideos, v.state()); err != nil {
		return err
	}
	if lifecycle == "" {
		return nil
	}
	e := v.state()
	e.Type = lifecycle
	return outbox.Insert(ctx, tx, events.TopicMediaEvents, e)
}

// ---- uploads ----

type UploadInput struct {
	Kind        string `json:"kind"`
	SizeBytes   int64  `json:"size_bytes"`
	ContentType string `json:"content_type"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Visibility  string `json:"visibility"`
}

type Upload struct {
	VideoID  string   `json:"video_id"`
	PartSize int64    `json:"part_size"`
	Parts    int      `json:"parts"`
	URLs     []string `json:"part_urls"` // first batch; fetch more with /parts
	Expires  int64    `json:"expires_at"`
}

var hashtagRe = regexp.MustCompile(`#([\p{L}\p{N}_]{1,50})`)

func (s *Service) CreateUpload(ctx context.Context, p auth.Principal, in UploadInput) (Upload, error) {
	limit, ok := maxSize[in.Kind]
	if !ok {
		return Upload{}, errs.BadRequest("kind_invalid", "Kind must be short or long.")
	}
	if in.SizeBytes <= 0 || in.SizeBytes > limit {
		return Upload{}, errs.BadRequest("size_invalid", fmt.Sprintf("Videos of this kind can be up to %d MB.", limit>>20))
	}
	if !slices.Contains([]string{"video/mp4", "video/quicktime", "video/webm"}, in.ContentType) {
		return Upload{}, errs.BadRequest("type_invalid", "Upload an MP4, MOV or WebM video.")
	}
	if utf8.RuneCountInString(in.Title) > 100 || utf8.RuneCountInString(in.Description) > 2200 {
		return Upload{}, errs.BadRequest("text_too_long", "Title max 100, description max 2200 characters.")
	}
	if in.Visibility == "" {
		in.Visibility = "public"
	}
	if !slices.Contains([]string{"public", "followers", "private"}, in.Visibility) {
		return Upload{}, errs.BadRequest("visibility_invalid", "Visibility must be public, followers or private.")
	}
	id := events.NewID()
	key := fmt.Sprintf("uploads/%s/%s/source", p.UserID, id)
	uploadID, err := s.S3.CreateMultipart(ctx, key, in.ContentType)
	if err != nil {
		return Upload{}, errs.Unavailable("storage_unavailable", "Uploads are unavailable right now.").Wrap(err)
	}
	parts := int((in.SizeBytes + partSize - 1) / partSize)
	expires := time.Now().Add(24 * time.Hour)
	var tags []string
	for _, m := range hashtagRe.FindAllStringSubmatch(in.Description, 30) {
		t := strings.ToLower(m[1])
		if !slices.Contains(tags, t) {
			tags = append(tags, t)
		}
	}
	err = db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO videos (id, owner_id, kind, status, visibility, title, description, hashtags, source_key)
			VALUES ($1, $2, $3, 'uploading', $4, $5, $6, $7, $8)`, id, p.UserID, in.Kind, in.Visibility, strings.TrimSpace(in.Title),
			strings.TrimSpace(in.Description), tags, key); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO uploads (video_id, upload_id, object_key, part_size, parts, expires_at) VALUES ($1, $2, $3, $4, $5, $6)`,
			id, uploadID, key, partSize, parts, expires)
		return err
	})
	if err != nil {
		return Upload{}, err
	}
	urls := s.partURLs(key, uploadID, 1, min(parts, 100))
	return Upload{VideoID: id, PartSize: partSize, Parts: parts, URLs: urls, Expires: expires.Unix()}, nil
}

func (s *Service) partURLs(key, uploadID string, from, to int) []string {
	out := make([]string, 0, to-from+1)
	for n := from; n <= to; n++ {
		out = append(out, s.S3.Presign("PUT", key, url.Values{"partNumber": {fmt.Sprint(n)}, "uploadId": {uploadID}}, 6*time.Hour))
	}
	return out
}

type upload struct {
	key, uploadID string
	parts         int
	completed     bool
}

func (s *Service) ownUpload(ctx context.Context, q db.Querier, owner, videoID string) (upload, error) {
	var u upload
	err := q.QueryRow(ctx, `SELECT u.object_key, u.upload_id, u.parts, u.completed FROM uploads u JOIN videos v ON v.id = u.video_id
		WHERE u.video_id = $1 AND v.owner_id = $2`, videoID, owner).Scan(&u.key, &u.uploadID, &u.parts, &u.completed)
	if db.IsNoRows(err) {
		return u, errs.ErrNotFound
	}
	return u, err
}

// PartURLs returns presigned URLs for parts [from, to] (max 200 per call).
func (s *Service) PartURLs(ctx context.Context, p auth.Principal, videoID string, from, to int) ([]string, error) {
	u, err := s.ownUpload(ctx, s.Pool, p.UserID, videoID)
	if err != nil {
		return nil, err
	}
	if from < 1 || to > u.parts || from > to || to-from >= 200 {
		return nil, errs.BadRequest("range_invalid", fmt.Sprintf("Part range must be within 1-%d, max 200 at a time.", u.parts))
	}
	return s.partURLs(u.key, u.uploadID, from, to), nil
}

// Complete finishes the multipart upload and queues transcoding.
func (s *Service) Complete(ctx context.Context, p auth.Principal, videoID string, parts []s3x.Part) (Video, error) {
	u, err := s.ownUpload(ctx, s.Pool, p.UserID, videoID)
	if err != nil {
		return Video{}, err
	}
	if !u.completed {
		if len(parts) != u.parts {
			return Video{}, errs.BadRequest("parts_missing", fmt.Sprintf("Expected %d parts, got %d.", u.parts, len(parts)))
		}
		if err := s.S3.CompleteMultipart(ctx, u.key, u.uploadID, parts); err != nil {
			return Video{}, errs.BadRequest("upload_incomplete", "Some parts didn't upload. Retry them, then complete again.").Wrap(err)
		}
	}
	var v Video
	err = db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE uploads SET completed = true WHERE video_id = $1`, videoID); err != nil {
			return err
		}
		v, err = scanVideo(tx.QueryRow(ctx, `UPDATE videos SET status = 'processing', updated_at = now()
			WHERE id = $1 AND status = 'uploading' RETURNING `+videoCols, videoID))
		if err == errs.ErrNotFound {
			v, err = scanVideo(tx.QueryRow(ctx, `SELECT `+videoCols+` FROM videos WHERE id = $1`, videoID))
			return err // already completed: idempotent
		}
		if err != nil {
			return err
		}
		return s.queueJob(ctx, tx, v, "uploaded")
	})
	return v, err
}

func (s *Service) queueJob(ctx context.Context, tx pgx.Tx, v Video, lifecycle string) error {
	if err := emit(ctx, tx, v, map[string]string{"uploaded": events.VideoUploaded}[lifecycle]); err != nil {
		return err
	}
	return outbox.Insert(ctx, tx, events.TopicMediaJobs, events.MustNew(producer, events.TranscodeJob, v.ID,
		events.TranscodeJobData{JobID: events.NewID(), VideoID: v.ID, Kind: v.Kind, SourceKey: v.sourceKey, OutputPrefix: "hls/" + v.ID}))
}

// ---- reads and edits ----

func (s *Service) Get(ctx context.Context, viewer, id string) (Video, error) {
	v, err := scanVideo(s.Pool.QueryRow(ctx, `SELECT `+videoCols+` FROM videos WHERE id = $1`, id))
	if err != nil {
		return v, err
	}
	if v.OwnerID != viewer && (v.Status != "ready" || v.Visibility == "private") {
		return Video{}, errs.ErrNotFound
	}
	return v, nil
}

func (s *Service) ByOwner(ctx context.Context, viewer, owner, kind, cursor string, limit int) ([]Video, string, error) {
	if limit <= 0 || limit > 50 {
		limit = 24
	}
	if cursor == "" {
		cursor = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	}
	if kind == "" {
		kind = "short"
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+videoCols+` FROM videos WHERE owner_id = $1 AND kind = $2 AND status = 'ready'
		AND (visibility = 'public' OR owner_id::text = $3) AND id < $4 ORDER BY id DESC LIMIT $5`, owner, kind, viewer, cursor, limit)
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

type Patch struct {
	Title       *string `json:"title,omitempty"`
	Description *string `json:"description,omitempty"`
	Visibility  *string `json:"visibility,omitempty"`
}

func (s *Service) Update(ctx context.Context, p auth.Principal, id string, in Patch) (Video, error) {
	if in.Visibility != nil && !slices.Contains([]string{"public", "followers", "private"}, *in.Visibility) {
		return Video{}, errs.BadRequest("visibility_invalid", "Visibility must be public, followers or private.")
	}
	var v Video
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		v, err = scanVideo(tx.QueryRow(ctx, `UPDATE videos SET title = COALESCE($3, title), description = COALESCE($4, description),
			visibility = COALESCE($5, visibility), updated_at = now() WHERE id = $1 AND owner_id = $2 AND status <> 'removed' RETURNING `+videoCols,
			id, p.UserID, in.Title, in.Description, in.Visibility))
		if err != nil {
			return err
		}
		return emit(ctx, tx, v, "")
	})
	return v, err
}

func (s *Service) Remove(ctx context.Context, id, owner, reason string) error {
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		v, err := scanVideo(tx.QueryRow(ctx, `UPDATE videos SET status = 'removed', updated_at = now()
			WHERE id = $1 AND ($2 = '' OR owner_id::text = $2) AND status <> 'removed' RETURNING `+videoCols, id, owner))
		if err == errs.ErrNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return emit(ctx, tx, v, events.VideoRemoved)
	})
}

// ---- consumers ----

// OnResult applies a transcode result (consumer of media.job_results).
func (s *Service) OnResult(ctx context.Context, e events.Envelope) error {
	var r events.TranscodeResultData
	if err := e.Decode(&r); err != nil {
		return nil
	}
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var v Video
		var err error
		if r.OK {
			v, err = scanVideo(tx.QueryRow(ctx, `UPDATE videos SET status = 'ready', hls_url = $2, thumbnail_url = $3, duration_ms = $4,
				width = $5, height = $6, published_at = COALESCE(published_at, now()), updated_at = now()
				WHERE id = $1 AND status = 'processing' RETURNING `+videoCols,
				r.VideoID, s.CDNURL+"/"+r.MasterKey, s.CDNURL+"/"+r.ThumbKey, r.DurationMS, r.Width, r.Height))
		} else {
			v, err = scanVideo(tx.QueryRow(ctx, `UPDATE videos SET status = 'failed', updated_at = now()
				WHERE id = $1 AND status = 'processing' RETURNING `+videoCols, r.VideoID))
		}
		if err == errs.ErrNotFound {
			return nil // duplicate or late result
		}
		if err != nil {
			return err
		}
		ev := events.VideoReady
		if !r.OK {
			ev = events.VideoFailed
		}
		return emit(ctx, tx, v, ev)
	})
}

// OnLive imports live replays (consumer of live.events).
func (s *Service) OnLive(ctx context.Context, e events.Envelope) error {
	if e.Type != events.ReplayReady {
		return nil
	}
	var r events.ReplayReadyData
	if err := e.Decode(&r); err != nil {
		return nil
	}
	return db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		v, err := scanVideo(tx.QueryRow(ctx, `INSERT INTO videos (id, owner_id, kind, status, title, source_key, source_ref)
			VALUES ($1, $2, 'replay', 'processing', $3, $4, $5) ON CONFLICT (source_ref) WHERE source_ref IS NOT NULL DO NOTHING
			RETURNING `+videoCols, events.NewID(), r.HostID, r.Title, r.ObjectKey, "live:"+r.RoomID))
		if err == errs.ErrNotFound {
			return nil // already imported
		}
		if err != nil {
			return err
		}
		return s.queueJob(ctx, tx, v, "")
	})
}

func (s *Service) OnModeration(ctx context.Context, e events.Envelope) error {
	var a events.ModerationActionData
	if err := e.Decode(&a); err != nil {
		return nil
	}
	if a.TargetType == "video" && a.Action == "remove" {
		return s.Remove(ctx, a.TargetID, "", "moderation")
	}
	return nil
}

// AbortStale cancels uploads that never completed (frees storage).
func (s *Service) AbortStale(ctx context.Context) error {
	rows, err := s.Pool.Query(ctx, `SELECT u.video_id::text, u.object_key, u.upload_id FROM uploads u
		WHERE NOT u.completed AND u.expires_at < now() LIMIT 500`)
	if err != nil {
		return err
	}
	type st struct{ id, key, up string }
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (st, error) {
		var x st
		return x, r.Scan(&x.id, &x.key, &x.up)
	})
	if err != nil {
		return err
	}
	for _, x := range list {
		_ = s.S3.AbortMultipart(ctx, x.key, x.up)
		_, _ = s.Pool.Exec(ctx, `UPDATE videos SET status = 'failed', updated_at = now() WHERE id = $1 AND status = 'uploading'`, x.id)
		_, _ = s.Pool.Exec(ctx, `DELETE FROM uploads WHERE video_id = $1`, x.id)
	}
	return nil
}
