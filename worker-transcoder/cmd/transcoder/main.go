// Command transcoder runs worker-transcoder (needs ffmpeg in the image).
package main

import (
	"context"
	"os"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/app"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/config"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/httpx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/logx"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/s3x"
	tapp "github.com/oviethebaddestcoder/lumo-backend/worker-transcoder/internal/app"
)

func main() {
	app.Main("worker-transcoder", app.Commands{"worker": worker})
}

// Scale on media.jobs consumer lag; each pod runs MAX_JOBS ffmpeg processes.
// Give pods CPU (e.g. 4 cores) and fast local disk for temp files.
func worker(ctx context.Context) error {
	e := config.New()
	logx.New("worker-transcoder", e.Str("LOG_LEVEL", "info"))
	addr := e.Str("ADDR", ":8080")
	kcfg := kafkax.ConfigFromEnv(e, "worker-transcoder")
	s3 := &s3x.Client{Endpoint: e.Required("S3_ENDPOINT"), Region: e.Str("S3_REGION", "auto"), Bucket: e.Required("S3_BUCKET"),
		AccessKey: e.Required("S3_ACCESS_KEY"), SecretKey: e.Required("S3_SECRET_KEY")}
	maxJobs := e.Int("MAX_JOBS", 2)
	tmp := e.Str("TMP_DIR", os.TempDir())
	if err := e.Err(); err != nil {
		return err
	}
	prod, err := kafkax.NewProducer(kcfg, "worker-transcoder")
	if err != nil {
		return err
	}
	defer prod.Close()
	w := &tapp.Worker{S3: s3, Producer: prod, TmpDir: tmp, Sem: make(chan struct{}, maxJobs)}
	c, err := kafkax.NewConsumer(kcfg, kafkax.ConsumerOptions{Group: "transcoder", Topics: []string{events.TopicMediaJobs},
		Handle: w.Handle, HandlerTimeout: 3 * time.Hour, MaxAttempts: 3})
	if err != nil {
		return err
	}
	return app.Group(ctx, c.Run, httpx.NewServer(addr, nil, httpx.Check{Name: "kafka", Fn: prod.Ping}).Run)
}
