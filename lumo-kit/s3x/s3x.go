// Package s3x signs S3-compatible requests (AWS S3, Cloudflare R2, MinIO)
// with AWS Signature V4, without pulling in the AWS SDK. It covers what the
// media pipeline needs: presigned PUT/GET, multipart uploads, and plain PUTs.
package s3x

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Endpoint  string // e.g. https://<account>.r2.cloudflarestorage.com or https://s3.eu-west-1.amazonaws.com
	Region    string // "auto" for R2
	Bucket    string
	AccessKey string
	SecretKey string
	HTTP      *http.Client
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 5 * time.Minute}
}

// objectURL uses path-style addressing (works on R2, MinIO and S3).
// With Bucket empty, Endpoint is a virtual-hosted bucket URL.
func (c *Client) objectURL(key string, q url.Values) *url.URL {
	u, _ := url.Parse(strings.TrimRight(c.Endpoint, "/"))
	prefix := "/"
	if c.Bucket != "" {
		prefix = "/" + c.Bucket + "/"
	}
	u.Path = prefix + strings.TrimLeft(key, "/")
	u.RawPath = prefix + escapePath(strings.TrimLeft(key, "/"))
	if q != nil {
		u.RawQuery = canonicalQuery(q)
	}
	return u
}

// Presign returns a URL valid for expiry that allows method on key.
// For multipart parts pass q = {"partNumber": n, "uploadId": id}.
func (c *Client) Presign(method, key string, q url.Values, expiry time.Duration) string {
	return c.PresignHeaders(method, key, q, expiry, nil)
}

// PresignHeaders is Presign with extra signed headers. The request must
// then carry exactly those headers, so signing Content-Length and
// Content-Type makes S3 refuse an upload of any other size or type than
// the one that was approved.
func (c *Client) PresignHeaders(method, key string, q url.Values, expiry time.Duration, headers map[string]string) string {
	now := nowFunc().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	scope := date + "/" + c.Region + "/s3/aws4_request"
	if q == nil {
		q = url.Values{}
	} else {
		q = cloneValues(q)
	}
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", c.AccessKey+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", strconv.Itoa(int(expiry.Seconds())))
	u := c.objectURL(key, nil)
	hs := map[string]string{"host": u.Host}
	for k, v := range headers {
		hs[strings.ToLower(k)] = strings.TrimSpace(v)
	}
	names := make([]string, 0, len(hs))
	for k := range hs {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, n := range names {
		canonHeaders.WriteString(n + ":" + hs[n] + "\n")
	}
	signed := strings.Join(names, ";")
	q.Set("X-Amz-SignedHeaders", signed)
	u = c.objectURL(key, q)
	canonical := strings.Join([]string{method, u.EscapedPath(), u.RawQuery, canonHeaders.String(), signed, "UNSIGNED-PAYLOAD"}, "\n")
	sig := c.signature(date, amzDate, scope, canonical)
	u.RawQuery += "&X-Amz-Signature=" + sig
	return u.String()
}

// Do sends a signed request with an in-memory body.
func (c *Client) Do(ctx context.Context, method, key string, q url.Values, body []byte, headers map[string]string) (*http.Response, error) {
	u := c.objectURL(key, q)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	scope := date + "/" + c.Region + "/s3/aws4_request"
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.ContentLength = int64(len(body))

	names := []string{"host"}
	vals := map[string]string{"host": u.Host}
	for k := range req.Header {
		lk := strings.ToLower(k)
		if lk == "x-amz-date" || lk == "x-amz-content-sha256" || lk == "content-type" || strings.HasPrefix(lk, "x-amz-meta-") || lk == "cache-control" {
			names = append(names, lk)
			vals[lk] = strings.TrimSpace(req.Header.Get(k))
		}
	}
	sort.Strings(names)
	var ch strings.Builder
	for _, n := range names {
		ch.WriteString(n + ":" + vals[n] + "\n")
	}
	signed := strings.Join(names, ";")
	canonical := strings.Join([]string{method, u.EscapedPath(), u.RawQuery, ch.String(), signed, payloadHash}, "\n")
	sig := c.signature(date, amzDate, scope, canonical)
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", c.AccessKey, scope, signed, sig))
	return c.http().Do(req)
}

// Put uploads a small object.
func (c *Client) Put(ctx context.Context, key string, body []byte, contentType, cacheControl string) error {
	h := map[string]string{"Content-Type": contentType}
	if cacheControl != "" {
		h["Cache-Control"] = cacheControl
	}
	resp, err := c.Do(ctx, http.MethodPut, key, nil, body, h)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3x: put %s: %d %s", key, resp.StatusCode, b)
	}
	return nil
}

// CreateMultipart starts a multipart upload and returns its upload ID.
func (c *Client) CreateMultipart(ctx context.Context, key, contentType string) (string, error) {
	resp, err := c.Do(ctx, http.MethodPost, key, url.Values{"uploads": {""}}, nil, map[string]string{"Content-Type": contentType})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("s3x: create multipart: %d %s", resp.StatusCode, b)
	}
	var out struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(b, &out); err != nil || out.UploadID == "" {
		return "", fmt.Errorf("s3x: create multipart: bad response: %s", b)
	}
	return out.UploadID, nil
}

type Part struct {
	Number int    `json:"number" xml:"PartNumber"`
	ETag   string `json:"etag" xml:"ETag"`
}

// CompleteMultipart finishes a multipart upload.
func (c *Client) CompleteMultipart(ctx context.Context, key, uploadID string, parts []Part) error {
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	body, _ := xml.Marshal(struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []Part   `xml:"Part"`
	}{Parts: parts})
	resp, err := c.Do(ctx, http.MethodPost, key, url.Values{"uploadId": {uploadID}}, body, map[string]string{"Content-Type": "application/xml"})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	// S3 can return 200 with an <Error> body.
	if resp.StatusCode/100 != 2 || bytes.Contains(b, []byte("<Error>")) {
		return fmt.Errorf("s3x: complete multipart: %d %s", resp.StatusCode, b)
	}
	return nil
}

// AbortMultipart cancels an upload (cleanup of abandoned uploads).
func (c *Client) AbortMultipart(ctx context.Context, key, uploadID string) error {
	resp, err := c.Do(ctx, http.MethodDelete, key, url.Values{"uploadId": {uploadID}}, nil, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

var nowFunc = time.Now

func (c *Client) signature(date, amzDate, scope, canonical string) string {
	h := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(h[:])
	k := hmacSHA256([]byte("AWS4"+c.SecretKey), date)
	k = hmacSHA256(k, c.Region)
	k = hmacSHA256(k, "s3")
	k = hmacSHA256(k, "aws4_request")
	return hex.EncodeToString(hmacSHA256(k, toSign))
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// canonicalQuery sorts keys and uses RFC 3986 encoding as SigV4 requires.
func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := q[k]
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, uriEncode(k)+"="+uriEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

func escapePath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = uriEncode(s)
	}
	return strings.Join(segs, "/")
}

func uriEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func cloneValues(v url.Values) url.Values {
	out := url.Values{}
	for k, vs := range v {
		out[k] = append([]string(nil), vs...)
	}
	return out
}
