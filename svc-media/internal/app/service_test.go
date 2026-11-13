package app_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/auth"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/db"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/s3x"
	"github.com/oviethebaddestcoder/lumo-backend/svc-media/internal/app"
	"github.com/oviethebaddestcoder/lumo-backend/svc-media/migrations"
)

func TestUploadToReadyAndReplayImport(t *testing.T) {
	pg := os.Getenv("LUMO_TEST_PG")
	if pg == "" {
		t.Skip("set LUMO_TEST_PG")
	}
	ctx := context.Background()
	admin, _ := pgx.Connect(ctx, pg)
	name := fmt.Sprintf("media_test_%d", time.Now().UnixNano())
	admin.Exec(ctx, "CREATE DATABASE "+name)
	defer func() {
		admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		admin.Close(context.Background())
	}()
	cfg, _ := pgx.ParseConfig(pg)
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, name)
	if err := db.Migrate(ctx, dsn, migrations.FS, migrations.LockID); err != nil {
		t.Fatal(err)
	}
	pool, _ := db.Open(ctx, dsn, db.Options{})
	defer pool.Close()

	// Fake S3: checks the request is signed and answers multipart calls.
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AK/") {
			w.WriteHeader(403)
			return
		}
		if _, ok := r.URL.Query()["uploads"]; ok {
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>UP123</UploadId></InitiateMultipartUploadResult>`))
			return
		}
		w.Write([]byte(`<CompleteMultipartUploadResult></CompleteMultipartUploadResult>`))
	}))
	defer s3.Close()
	svc := &app.Service{Pool: pool, CDNURL: "https://cdn.test",
		S3: &s3x.Client{Endpoint: s3.URL, Region: "auto", Bucket: "media", AccessKey: "AK", SecretKey: "SK"}}
	owner := auth.Principal{UserID: events.NewID()}

	up, err := svc.CreateUpload(ctx, owner, app.UploadInput{Kind: "short", SizeBytes: 40 << 20, ContentType: "video/mp4",
		Description: "Lagos sunset #AfroDance #lagos #afrodance"})
	if err != nil {
		t.Fatal(err)
	}
	if up.Parts != 3 || len(up.URLs) != 3 || !strings.Contains(up.URLs[0], "partNumber=1") || !strings.Contains(up.URLs[0], "X-Amz-Signature=") {
		t.Fatalf("upload: %+v", up)
	}
	if _, err := svc.Complete(ctx, owner, up.VideoID, []s3x.Part{{Number: 1, ETag: "a"}}); err == nil {
		t.Fatal("completed with missing parts")
	}
	if _, err := svc.Complete(ctx, auth.Principal{UserID: events.NewID()}, up.VideoID, nil); err == nil {
		t.Fatal("someone else completed the upload")
	}
	v, err := svc.Complete(ctx, owner, up.VideoID, []s3x.Part{{Number: 1, ETag: "a"}, {Number: 2, ETag: "b"}, {Number: 3, ETag: "c"}})
	if err != nil || v.Status != "processing" {
		t.Fatalf("complete: %v %+v", err, v)
	}
	if len(v.Hashtags) != 2 {
		t.Fatalf("hashtags %v", v.Hashtags)
	}
	// Not visible to others until ready.
	if _, err := svc.Get(ctx, "", up.VideoID); err == nil {
		t.Fatal("processing video visible to public")
	}
	res := events.MustNew("worker-transcoder", events.TranscodeDone, up.VideoID, events.TranscodeResultData{VideoID: up.VideoID, OK: true,
		MasterKey: "hls/" + up.VideoID + "/master.m3u8", ThumbKey: "hls/" + up.VideoID + "/thumb.jpg", DurationMS: 15000, Width: 1080, Height: 1920})
	if err := svc.OnResult(ctx, res); err != nil {
		t.Fatal(err)
	}
	svc.OnResult(ctx, res) // duplicate delivery is a no-op
	got, err := svc.Get(ctx, "", up.VideoID)
	if err != nil || got.Status != "ready" || got.HLSURL != "https://cdn.test/hls/"+up.VideoID+"/master.m3u8" {
		t.Fatalf("ready: %v %+v", err, got)
	}
	var ready int
	pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE payload->>'type' = 'media.video.ready'`).Scan(&ready)
	if ready != 1 {
		t.Fatalf("ready events %d", ready)
	}
	// Live replay is imported once even if the event repeats.
	rep := events.MustNew("svc-live", events.ReplayReady, "room1", events.ReplayReadyData{RoomID: "room1", HostID: owner.UserID, Title: "Karaoke", ObjectKey: "replays/x.mp4"})
	svc.OnLive(ctx, rep)
	svc.OnLive(ctx, rep)
	var replays int
	pool.QueryRow(ctx, `SELECT count(*) FROM videos WHERE kind = 'replay'`).Scan(&replays)
	if replays != 1 {
		t.Fatalf("replays %d", replays)
	}

	// Views: only plays of 1 s or more count, and redelivery doesn't double count.
	imp := func(ms int64) events.Envelope {
		return events.MustNew("svc-feed", events.Impression, "u", events.ImpressionData{UserID: events.NewID(), VideoID: up.VideoID, WatchMS: ms})
	}
	batch := []events.Envelope{imp(5000), imp(1200), imp(300)}
	if err := svc.ApplyViews(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := svc.ApplyViews(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if got, _ := svc.Get(ctx, "", up.VideoID); got.Views != 2 {
		t.Fatalf("views %d, want 2", got.Views)
	}
	// Hashtag search is case-insensitive and only shows public ready videos.
	tagged, _, err := svc.ByHashtag(ctx, "#AfroDance", "", 10)
	if err != nil || len(tagged) != 1 || tagged[0].ID != up.VideoID {
		t.Fatalf("hashtag: %v %d", err, len(tagged))
	}
	// Image uploads are namespaced under the caller and type-checked.
	img, err := svc.CreateImageUpload(ctx, owner, app.ImageInput{Purpose: "avatar", ContentType: "image/jpeg", SizeBytes: 200_000})
	if err != nil || !strings.HasPrefix(img.URL, "https://cdn.test/images/avatar/"+owner.UserID+"/") || !strings.Contains(img.UploadURL, "X-Amz-Signature=") {
		t.Fatalf("image: %v %+v", err, img)
	}
	if _, err := svc.CreateImageUpload(ctx, owner, app.ImageInput{Purpose: "avatar", ContentType: "image/gif", SizeBytes: 10}); err == nil {
		t.Fatal("gif accepted")
	}
	if _, err := svc.CreateImageUpload(ctx, owner, app.ImageInput{Purpose: "avatar", ContentType: "image/png", SizeBytes: 6 << 20}); err == nil {
		t.Fatal("oversized image accepted")
	}
}
