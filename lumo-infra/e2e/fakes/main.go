// Command fakes runs in-memory stand-ins for the third parties the
// end-to-end test needs: Kafka (franz-go kfake), S3 (just the calls Lumo
// makes), Paystack and the LiveKit server API. Nothing here is for
// production; it exists so the real services can run together on one
// machine with no Docker.
package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oviethebaddestcoder/lumo-backend/lumo-contracts/events"
	"github.com/twmb/franz-go/pkg/kfake"
)

func main() {
	kafkaPort := flag.Int("kafka", 19092, "Kafka port")
	s3Addr := flag.String("s3", ":19000", "S3 address")
	paystackAddr := flag.String("paystack", ":19100", "Paystack address")
	livekitAddr := flag.String("livekit", ":19200", "LiveKit API address")
	partitions := flag.Int("partitions", 4, "partitions per topic")
	flag.Parse()

	topics := make([]string, 0, len(events.AllTopics))
	for _, t := range events.AllTopics {
		topics = append(topics, t.Name, t.Name+".dlq")
	}
	c, err := kfake.NewCluster(kfake.Ports(*kafkaPort), kfake.SeedTopics(int32(*partitions), topics...), kfake.AllowAutoTopicCreation())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	log.Printf("kafka on %v (%d topics)", c.ListenAddrs(), len(topics))

	go serve(*s3Addr, newS3())
	go serve(*paystackAddr, newPaystack())
	var ingresses atomic.Int64
	go serve(*livekitAddr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Twirp-style LiveKit server API. Ingress creation returns a real-looking
		// RTMP endpoint; everything else answers with an empty object.
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/livekit.Ingress/CreateIngress") {
			n := ingresses.Add(1)
			fmt.Fprintf(w, `{"ingress_id":"IN_%d","url":"rtmps://ingest.lumo.test:443/x","stream_key":"sk_%d_%d"}`, n, n, time.Now().UnixNano()%1e6)
			return
		}
		w.Write([]byte(`{}`))
	}))
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

func serve(addr string, h http.Handler) {
	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, h))
}

// ---- S3 ----

type s3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	types   map[string]string
	uploads map[string]map[int][]byte
	next    atomic.Int64
}

func newS3() *s3 {
	return &s3{objects: map[string][]byte{}, types: map[string]string{}, uploads: map[string]map[int][]byte{}}
}

func etag(b []byte) string {
	h := md5.Sum(b)
	return `"` + hex.EncodeToString(h[:]) + `"`
}

func (s *s3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "ETag")
	key := r.URL.Path // "/bucket/key..."
	q := r.URL.Query()
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		id := strconv.FormatInt(s.next.Add(1), 10)
		s.uploads[id] = map[int][]byte{}
		fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, id)
	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		parts, ok := s.uploads[q.Get("uploadId")]
		if !ok {
			http.Error(w, "NoSuchUpload", http.StatusNotFound)
			return
		}
		n, _ := strconv.Atoi(q.Get("partNumber"))
		parts[n] = body
		w.Header().Set("ETag", etag(body))
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		parts, ok := s.uploads[q.Get("uploadId")]
		if !ok {
			http.Error(w, "NoSuchUpload", http.StatusNotFound)
			return
		}
		var in struct {
			Parts []struct {
				PartNumber int
				ETag       string
			} `xml:"Part"`
		}
		if err := xml.Unmarshal(body, &in); err != nil || len(in.Parts) == 0 {
			http.Error(w, "MalformedXML", http.StatusBadRequest)
			return
		}
		sort.Slice(in.Parts, func(i, j int) bool { return in.Parts[i].PartNumber < in.Parts[j].PartNumber })
		var all bytes.Buffer
		for _, p := range in.Parts {
			b, ok := parts[p.PartNumber]
			if !ok || etag(b) != p.ETag {
				http.Error(w, "InvalidPart", http.StatusBadRequest)
				return
			}
			all.Write(b)
		}
		s.objects[key] = all.Bytes()
		delete(s.uploads, q.Get("uploadId"))
		fmt.Fprintf(w, `<CompleteMultipartUploadResult><ETag>%s</ETag></CompleteMultipartUploadResult>`, etag(all.Bytes()))
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		delete(s.uploads, q.Get("uploadId"))
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut:
		// Like S3, enforce a signed Content-Length.
		if want := q.Get("X-Amz-SignedHeaders"); strings.Contains(want, "content-length") && r.ContentLength != int64(len(body)) {
			http.Error(w, "SignatureDoesNotMatch", http.StatusForbidden)
			return
		}
		s.objects[key] = body
		s.types[key] = r.Header.Get("Content-Type")
		w.Header().Set("ETag", etag(body))
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		b, ok := s.objects[key]
		if !ok {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		if t := s.types[key]; t != "" {
			w.Header().Set("Content-Type", t)
		}
		w.Header().Set("ETag", etag(b))
		// Range requests matter: ffmpeg seeks inside MP4 sources.
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(b))
	case r.Method == http.MethodDelete:
		delete(s.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "NotImplemented", http.StatusNotImplemented)
	}
}

// ---- Paystack ----

type tx struct {
	Reference string         `json:"reference"`
	Amount    int64          `json:"amount"`
	Currency  string         `json:"currency"`
	Status    string         `json:"status"`
	Metadata  map[string]any `json:"metadata"`
}

type paystack struct {
	mu        sync.Mutex
	txs       map[string]*tx
	transfers map[string]int64 // reference -> amount
}

func newPaystack() *paystack {
	return &paystack{txs: map[string]*tx{}, transfers: map[string]int64{}}
}

func (p *paystack) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Header.Get("Authorization") == "" && !strings.HasPrefix(r.URL.Path, "/__") {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"status":false,"message":"no key"}`))
		return
	}
	var in map[string]any
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&in)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	reply := func(data any) {
		json.NewEncoder(w).Encode(map[string]any{"status": true, "message": "ok", "data": data})
	}
	switch {
	case r.URL.Path == "/transaction/initialize":
		ref, _ := in["reference"].(string)
		meta, _ := in["metadata"].(map[string]any)
		amount, _ := in["amount"].(float64)
		p.txs[ref] = &tx{Reference: ref, Amount: int64(amount), Currency: "NGN", Status: "abandoned", Metadata: meta}
		reply(map[string]any{"authorization_url": "https://checkout.paystack.test/" + ref, "reference": ref})
	case strings.HasPrefix(r.URL.Path, "/transaction/verify/"):
		t, ok := p.txs[strings.TrimPrefix(r.URL.Path, "/transaction/verify/")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"status":false,"message":"Transaction reference not found"}`))
			return
		}
		reply(t)
	case strings.HasPrefix(r.URL.Path, "/__pay/"):
		// Test hook: the customer completed checkout.
		if t, ok := p.txs[strings.TrimPrefix(r.URL.Path, "/__pay/")]; ok {
			t.Status = "success"
			reply(t)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case r.URL.Path == "/__transfers":
		reply(p.transfers)
	case r.URL.Path == "/bank":
		reply([]map[string]any{{"code": "058", "name": "GTBank", "active": true}, {"code": "044", "name": "Access Bank", "active": true}})
	case r.URL.Path == "/bank/resolve":
		reply(map[string]any{"account_name": "ADA OBI", "account_number": r.URL.Query().Get("account_number")})
	case r.URL.Path == "/transferrecipient":
		reply(map[string]any{"recipient_code": fmt.Sprintf("RCP_%v", in["account_number"])})
	case r.URL.Path == "/transfer":
		ref, _ := in["reference"].(string)
		if _, dup := p.transfers[ref]; dup {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"status":false,"message":"Duplicate Transfer Reference"}`))
			return
		}
		amount, _ := in["amount"].(float64)
		p.transfers[ref] = int64(amount)
		reply(map[string]any{"status": "pending", "reference": ref, "transfer_code": "TRF_" + ref[:8], "createdAt": time.Now()})
	default:
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"status":false,"message":"not faked"}`))
	}
}
