package s3x

import (
	"strings"
	"testing"
	"time"
)

// AWS's published SigV4 presigned-URL example (S3 docs, "Authenticating
// Requests: Using Query Parameters").
func TestPresignMatchesAWSExample(t *testing.T) {
	nowFunc = func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) }
	defer func() { nowFunc = time.Now }()
	c := &Client{Endpoint: "https://examplebucket.s3.amazonaws.com", Region: "us-east-1",
		AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
	u := c.Presign("GET", "test.txt", nil, 86400*time.Second)
	want := "X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	if !strings.HasSuffix(u, want) {
		t.Fatalf("got %s", u)
	}
}

func TestPresignHeadersSignsSizeAndType(t *testing.T) {
	c := &Client{Endpoint: "https://bucket.s3.amazonaws.com", Region: "eu-west-1", AccessKey: "AK", SecretKey: "SK"}
	a := c.PresignHeaders("PUT", "images/a.jpg", nil, time.Minute, map[string]string{"Content-Length": "100", "Content-Type": "image/jpeg"})
	b := c.PresignHeaders("PUT", "images/a.jpg", nil, time.Minute, map[string]string{"Content-Length": "101", "Content-Type": "image/jpeg"})
	if !strings.Contains(a, "X-Amz-SignedHeaders=content-length%3Bcontent-type%3Bhost") {
		t.Fatalf("signed headers missing: %s", a)
	}
	sig := func(u string) string { return u[strings.LastIndex(u, "X-Amz-Signature=")+16:] }
	if sig(a) == sig(b) {
		t.Fatal("a different size must change the signature")
	}
	if plain := c.Presign("PUT", "images/a.jpg", nil, time.Minute); !strings.Contains(plain, "X-Amz-SignedHeaders=host") {
		t.Fatalf("plain presign changed: %s", plain)
	}
}
