// Package app is worker-transcoder: it turns uploaded videos into adaptive
// HLS (several resolutions) plus a thumbnail, and reports the result.
//
// It is stateless: output paths are derived from the video ID, so a job that
// runs twice (Kafka redelivery, pod restart) simply overwrites the same files.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/kafkax"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/metrics"
	"github.com/oviethebaddestcoder/lumo-backend/lumo-kit/s3x"
	"golang.org/x/sync/errgroup"
)

var (
	jobs       = metrics.NewCounterVec("transcode_jobs_total", "Transcode jobs by result.", "result")
	jobSeconds = metrics.NewHistogramVec("transcode_job_seconds", "Wall time per job.", "kind")
)

type Rung struct {
	Short   int // pixels on the short side
	Bitrate int // kbps
}

// Ladder: 360p is the data-saver default on mobile networks.
var Ladder = []Rung{{360, 800}, {540, 1500}, {720, 2800}, {1080, 5000}}

var maxDuration = map[string]time.Duration{"short": 10 * time.Minute, "long": 4 * time.Hour, "replay": 8 * time.Hour}

type Worker struct {
	S3       *s3x.Client
	Producer *kafkax.Producer
	TmpDir   string
	Sem      chan struct{} // limits concurrent ffmpeg jobs per pod
}

type Probe struct {
	Width, Height int
	DurationMS    int64
	HasAudio      bool
}

func ProbeFile(ctx context.Context, src string) (Probe, error) {
	out, err := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries",
		"stream=codec_type,width,height:format=duration", "-of", "json", src).Output()
	if err != nil {
		return Probe{}, fmt.Errorf("ffprobe: %w", err)
	}
	var r struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
			Width     int    `json:"width"`
			Height    int    `json:"height"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return Probe{}, err
	}
	var p Probe
	for _, s := range r.Streams {
		switch s.CodecType {
		case "video":
			if p.Width == 0 {
				p.Width, p.Height = s.Width, s.Height
			}
		case "audio":
			p.HasAudio = true
		}
	}
	d, _ := strconv.ParseFloat(r.Format.Duration, 64)
	p.DurationMS = int64(d * 1000)
	if p.Width == 0 || p.Height == 0 {
		return p, fmt.Errorf("no video stream")
	}
	return p, nil
}

// rungsFor picks every rung at or below the source resolution (at least one).
func rungsFor(p Probe) []Rung {
	short := min(p.Width, p.Height)
	var rs []Rung
	for _, r := range Ladder {
		if r.Short <= short {
			rs = append(rs, r)
		}
	}
	if len(rs) == 0 {
		rs = Ladder[:1]
	}
	return rs
}

// Encode writes master.m3u8, v<N>/index.m3u8 + segments and thumb.jpg into dir.
func Encode(ctx context.Context, src, dir string, p Probe) error {
	rs := rungsFor(p)
	portrait := p.Height > p.Width
	var filter strings.Builder
	fmt.Fprintf(&filter, "[0:v]split=%d", len(rs))
	for i := range rs {
		fmt.Fprintf(&filter, "[s%d]", i)
	}
	for i, r := range rs {
		scale := fmt.Sprintf("scale=-2:%d", r.Short)
		if portrait {
			scale = fmt.Sprintf("scale=%d:-2", r.Short)
		}
		fmt.Fprintf(&filter, ";[s%d]%s[o%d]", i, scale, i)
	}
	args := []string{"-y", "-hide_banner", "-loglevel", "error", "-i", src, "-filter_complex", filter.String()}
	var streamMap []string
	for i, r := range rs {
		args = append(args, "-map", fmt.Sprintf("[o%d]", i))
		if p.HasAudio {
			args = append(args, "-map", "0:a:0")
		}
		args = append(args,
			fmt.Sprintf("-b:v:%d", i), fmt.Sprintf("%dk", r.Bitrate),
			fmt.Sprintf("-maxrate:v:%d", i), fmt.Sprintf("%dk", r.Bitrate*107/100),
			fmt.Sprintf("-bufsize:v:%d", i), fmt.Sprintf("%dk", r.Bitrate*3/2))
		m := fmt.Sprintf("v:%d", i)
		if p.HasAudio {
			m += fmt.Sprintf(",a:%d", i)
		}
		streamMap = append(streamMap, m+",name:"+strconv.Itoa(r.Short)+"p")
	}
	args = append(args,
		"-c:v", "libx264", "-preset", "veryfast", "-profile:v", "main", "-pix_fmt", "yuv420p",
		"-g", "48", "-keyint_min", "48", "-sc_threshold", "0")
	if p.HasAudio {
		args = append(args, "-c:a", "aac", "-b:a", "128k", "-ac", "2")
	}
	args = append(args, "-f", "hls", "-hls_time", "4", "-hls_playlist_type", "vod", "-hls_flags", "independent_segments",
		"-hls_segment_filename", filepath.Join(dir, "%v", "seg_%04d.ts"),
		"-master_pl_name", "master.m3u8", "-var_stream_map", strings.Join(streamMap, " "),
		filepath.Join(dir, "%v", "index.m3u8"))
	if err := run(ctx, "ffmpeg", args...); err != nil {
		return err
	}
	at := min(1.0, float64(p.DurationMS)/2000)
	return run(ctx, "ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-ss", fmt.Sprintf("%.2f", at), "-i", src,
		"-frames:v", "1", "-vf", "scale=480:-2", "-q:v", "4", filepath.Join(dir, "thumb.jpg"))
}

func run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := stderr.String()
		if len(msg) > 1500 {
			msg = msg[len(msg)-1500:]
		}
		return fmt.Errorf("%s: %w: %s", name, err, msg)
	}
	return nil
}

// Handle processes one media.jobs event.
func (w *Worker) Handle(ctx context.Context, e events.Envelope) error {
	if e.Type != events.TranscodeJob {
		return nil
	}
	var j events.TranscodeJobData
	if err := e.Decode(&j); err != nil {
		return kafkax.Permanent(err)
	}
	select {
	case w.Sem <- struct{}{}:
		defer func() { <-w.Sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	start := time.Now()
	res := events.TranscodeResultData{JobID: j.JobID, VideoID: j.VideoID}
	err := w.process(ctx, j, &res)
	if ctx.Err() != nil {
		return ctx.Err() // shutting down: let another pod redo it
	}
	res.OK = err == nil
	if err != nil {
		res.Error = err.Error()
		slog.ErrorContext(ctx, "transcode failed", "video_id", j.VideoID, "err", err)
		jobs.WithLabelValues("failed").Inc()
	} else {
		jobs.WithLabelValues("ok").Inc()
		jobSeconds.WithLabelValues(j.Kind).Observe(time.Since(start).Seconds())
	}
	return w.Producer.Publish(ctx, events.TopicMediaResults, events.MustNew("worker-transcoder", events.TranscodeDone, j.VideoID, res))
}

func (w *Worker) process(ctx context.Context, j events.TranscodeJobData, res *events.TranscodeResultData) error {
	dir, err := os.MkdirTemp(w.TmpDir, "job-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	src := w.S3.Presign("GET", j.SourceKey, nil, 3*time.Hour)
	p, err := ProbeFile(ctx, src)
	if err != nil {
		return err
	}
	if limit, ok := maxDuration[j.Kind]; ok && time.Duration(p.DurationMS)*time.Millisecond > limit {
		return fmt.Errorf("video too long: %dms", p.DurationMS)
	}
	if err := Encode(ctx, src, dir, p); err != nil {
		return err
	}
	if err := w.upload(ctx, dir, j.OutputPrefix); err != nil {
		return err
	}
	res.MasterKey, res.ThumbKey = j.OutputPrefix+"/master.m3u8", j.OutputPrefix+"/thumb.jpg"
	res.DurationMS, res.Width, res.Height = p.DurationMS, p.Width, p.Height
	return nil
}

func (w *Worker) upload(ctx context.Context, dir, prefix string) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		key := prefix + "/" + filepath.ToSlash(rel)
		g.Go(func() error {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return w.S3.Put(gctx, key, b, contentType(path), "public, max-age=31536000, immutable")
		})
		return nil
	})
	if err != nil {
		return err
	}
	return g.Wait()
}

func contentType(p string) string {
	switch filepath.Ext(p) {
	case ".m3u8":
		return "application/vnd.apple.mpegurl"
	case ".ts":
		return "video/mp2t"
	}
	if t := mime.TypeByExtension(filepath.Ext(p)); t != "" {
		return t
	}
	return "application/octet-stream"
}
