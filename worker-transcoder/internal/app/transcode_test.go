package app

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Encodes a real 3-second portrait clip with audio and checks the ladder.
func TestEncodeLadder(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	ctx := context.Background()
	dir := t.TempDir()
	src := filepath.Join(dir, "in.mp4")
	if err := run(ctx, "ffmpeg", "-y", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=720x1280:rate=30:duration=3",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=3", "-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac", "-shortest", src); err != nil {
		t.Fatal(err)
	}
	p, err := ProbeFile(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if p.Width != 720 || p.Height != 1280 || !p.HasAudio || p.DurationMS < 2900 {
		t.Fatalf("probe: %+v", p)
	}
	out := filepath.Join(dir, "out")
	if err := Encode(ctx, src, out, p); err != nil {
		t.Fatal(err)
	}
	master, err := os.ReadFile(filepath.Join(out, "master.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	// 720 short side -> 360p, 540p, 720p (no upscale to 1080p).
	for _, want := range []string{"360p/index.m3u8", "540p/index.m3u8", "720p/index.m3u8"} {
		if !strings.Contains(string(master), want) {
			t.Fatalf("master missing %s:\n%s", want, master)
		}
	}
	if strings.Contains(string(master), "1080p") {
		t.Fatal("upscaled to 1080p")
	}
	if !strings.Contains(string(master), "RESOLUTION=360x640") {
		t.Fatalf("portrait 360p should be 360x640:\n%s", master)
	}
	if _, err := os.Stat(filepath.Join(out, "thumb.jpg")); err != nil {
		t.Fatal("no thumbnail")
	}
	segs, _ := filepath.Glob(filepath.Join(out, "720p", "seg_*.ts"))
	if len(segs) == 0 {
		t.Fatal("no segments")
	}
}
