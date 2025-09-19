package promrw

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang/snappy"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

// Receiver implements a Prometheus Remote Write-compatible HTTP endpoint.
// Default path: POST /api/v1/write
// Content-Type: application/x-protobuf
// Content-Encoding: snappy | gzip | (none)
//
// It forwards the *decompressed* protobuf bytes of prompb.WriteRequest as
// model.Envelope{Kind: model.KindPromRW, Bytes: <raw protobuf>}.
type Receiver struct {
	endpoint string // host:port, e.g. ":19291"
	path     string // default "/api/v1/write"

	// Limits / timeouts
	maxBodyBytes int64
	readTimeout  time.Duration
	writeTimeout time.Duration
	idleTimeout  time.Duration

	// TLS / mTLS
	tlsEnabled        bool
	tlsCertFile       string
	tlsKeyFile        string
	tlsClientCAFile   string
	requireClientCert bool
}

// New builds a Prometheus Remote Write receiver.
//
// Supported rc.Extra keys:
//   - path: string (default "/api/v1/write")
//   - max_body_bytes: int (default 16*1024*1024)
//   - read_timeout_ms: int (default 30000)
//   - write_timeout_ms: int (default 30000)
//   - idle_timeout_ms: int (default 120000)
//   - tls.enabled: bool
//   - tls.cert_file: string
//   - tls.key_file: string
//   - tls.client_ca_file: string
//   - tls.require_client_cert: bool
func New(rc config.ReceiverCfg) *Receiver {
	path := "/api/v1/write"
	if s, ok := rc.Extra["path"].(string); ok && strings.TrimSpace(s) != "" {
		path = s
	}

	maxBody := int64(16 * 1024 * 1024)
	if v, ok := rc.Extra["max_body_bytes"].(int); ok && v > 0 {
		maxBody = int64(v)
	}

	rt := 30 * time.Second
	if v, ok := rc.Extra["read_timeout_ms"].(int); ok && v > 0 {
		rt = time.Duration(v) * time.Millisecond
	}
	wt := 30 * time.Second
	if v, ok := rc.Extra["write_timeout_ms"].(int); ok && v > 0 {
		wt = time.Duration(v) * time.Millisecond
	}
	it := 120 * time.Second
	if v, ok := rc.Extra["idle_timeout_ms"].(int); ok && v > 0 {
		it = time.Duration(v) * time.Millisecond
	}

	tlsEnabled := false
	if b, ok := nestedBool(rc.Extra, "tls", "enabled"); ok {
		tlsEnabled = b
	}
	certFile := nestedString(rc.Extra, "tls", "cert_file")
	keyFile := nestedString(rc.Extra, "tls", "key_file")
	caFile := nestedString(rc.Extra, "tls", "client_ca_file")
	requireClientCert := false
	if b, ok := nestedBool(rc.Extra, "tls", "require_client_cert"); ok {
		requireClientCert = b
	}

	return &Receiver{
		endpoint:          rc.Endpoint,
		path:              path,
		maxBodyBytes:      maxBody,
		readTimeout:       rt,
		writeTimeout:      wt,
		idleTimeout:       it,
		tlsEnabled:        tlsEnabled,
		tlsCertFile:       certFile,
		tlsKeyFile:        keyFile,
		tlsClientCAFile:   caFile,
		requireClientCert: requireClientCert,
	}
}

// Start launches the HTTP server and forwards requests into the pipeline.
// It responds 200 OK as soon as the body is read (backpressure-safe).
func (r *Receiver) Start(ctx context.Context, out chan<- model.Envelope) error {
	addr := r.endpoint
	if strings.TrimSpace(addr) == "" {
		addr = ":19291"
	}

	mux := http.NewServeMux()
	// Health
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	// Remote Write
	mux.HandleFunc(r.path, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Bound body
		var reader io.Reader = http.MaxBytesReader(w, req.Body, r.maxBodyBytes)
		defer req.Body.Close()

		// Decode based on Content-Encoding (Prom typically sends "snappy")
		encoding := strings.ToLower(req.Header.Get("Content-Encoding"))

		// If gzip first, unwrap to get the (possibly snappy) inner body.
		if strings.Contains(encoding, "gzip") {
			gr, err := gzip.NewReader(reader)
			if err != nil {
				http.Error(w, "invalid gzip", http.StatusBadRequest)
				return
			}
			defer gr.Close()
			reader = gr
			// fallthrough: the inner stream may still be snappy-compressed or raw protobuf
			encoding = strings.ReplaceAll(encoding, "gzip", "")
		}

		body, err := io.ReadAll(reader)
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}

		// Some senders omit Content-Encoding but still send snappy -> try to detect.
		decompressed := body
		if strings.Contains(encoding, "snappy") || looksSnappy(body) {
			decompressed, err = snappy.Decode(nil, body)
			if err != nil {
				http.Error(w, "invalid snappy", http.StatusBadRequest)
				return
			}
		}

		// Non-blocking forward (drop on backpressure, but still 200 OK)
		env := model.Envelope{
			Kind:   model.KindPromRW,
			Bytes:  decompressed, // raw prompb.WriteRequest
			Attrs:  extractPromHeaders(req),
			TSUnix: time.Now().Unix(),
		}
		select {
		case out <- env:
		default:
			log.Printf("[promrw] dropping request due to backpressure")
		}

		// Remote Write expects 200 OK on success.
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  r.readTimeout,
		WriteTimeout: r.writeTimeout,
		IdleTimeout:  r.idleTimeout,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	if r.tlsEnabled {
		tlsCfg, err := r.buildTLS()
		if err != nil {
			return fmt.Errorf("promrw tls: %w", err)
		}
		srv.TLSConfig = tlsCfg
		log.Printf("[promrw] listening on https://%s path=%s", addr, r.path)
	} else {
		log.Printf("[promrw] listening on http://%s path=%s", addr, r.path)
	}

	errCh := make(chan error, 1)
	go func() {
		var serveErr error
		if r.tlsEnabled {
			serveErr = srv.ServeTLS(ln, "", "") // certs from TLSConfig
		} else {
			serveErr = srv.Serve(ln)
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- serveErr
		}
	}()

	select {
	case <-ctx.Done():
		shctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shctx)
		return nil
	case e := <-errCh:
		return e
	}
}

// buildTLS constructs server TLS (and optional mTLS) config.
func (r *Receiver) buildTLS() (*tls.Config, error) {
	if r.tlsCertFile == "" || r.tlsKeyFile == "" {
		return nil, errors.New("cert_file and key_file are required when tls.enabled=true")
	}
	cert, err := tls.LoadX509KeyPair(r.tlsCertFile, r.tlsKeyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if r.tlsClientCAFile != "" {
		pem, err := os.ReadFile(r.tlsClientCAFile)
		if err != nil {
			return nil, err
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(pem) {
			return nil, errors.New("failed to append client CA")
		}
		cfg.ClientCAs = cp
		if r.requireClientCert {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			cfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return cfg, nil
}

// looksSnappy heuristically detects Prometheus-style snappy frames when the header is missing.
func looksSnappy(b []byte) bool {
	// Prom remote write uses snappy block format; small heuristic:
	// Try a quick decode of the first few bytes without allocating huge memory.
	if len(b) < 8 {
		return false
	}
	// snappy.Decode will error quickly if not snappy; keep this fast path tiny.
	_, err := snappy.Decode(nil, b[:min(len(b), 256)])
	return err == nil
}

func extractPromHeaders(req *http.Request) map[string]string {
	m := map[string]string{}
	// Common headers to preserve in attrs (useful for routing/tenancy)
	if v := req.Header.Get("X-Prometheus-Remote-Write-Version"); v != "" {
		m["promrw.version"] = v
	}
	if v := req.Header.Get("X-Prometheus-Scrape-Timeout-Seconds"); v != "" {
		m["promrw.scrape_timeout_s"] = v
	}
	// Often used for tenancy/auth (forward if set)
	if v := req.Header.Get("X-Scope-OrgID"); v != "" {
		m["org_id"] = v
	}
	if v := req.Header.Get("Authorization"); v != "" {
		m["authorization"] = v // consider masking in logs
	}
	return m
}

// ----------------- helpers -----------------

func nestedString(m map[string]any, k1, k2 string) string {
	if m == nil {
		return ""
	}
	n1, ok := m[k1].(map[string]any)
	if !ok {
		return ""
	}
	if s, ok := n1[k2].(string); ok {
		return s
	}
	return ""
}

func nestedBool(m map[string]any, k1, k2 string) (bool, bool) {
	if m == nil {
		return false, false
	}
	n1, ok := m[k1].(map[string]any)
	if !ok {
		return false, false
	}
	b, ok := n1[k2].(bool)
	return b, ok
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
