package otlphttp

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

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

// Receiver implements the OTLP/HTTP spec endpoints:
//
//	POST /v1/traces   (ExportTracesServiceRequest)
//	POST /v1/metrics  (ExportMetricsServiceRequest)
//	POST /v1/logs     (ExportLogsServiceRequest)
//
// Content-Type: application/x-protobuf (default per spec)
// Content-Encoding: gzip (optional)
// NOTE: We also accept application/json bodies and forward them raw; downstream
// processors must be able to handle JSON if you intend to use it.
type Receiver struct {
	endpoint string // host:port, e.g. ":4318"

	// Limits / timeouts
	maxBodyBytes int64         // default 16 MiB
	readTimeout  time.Duration // default 30s
	writeTimeout time.Duration // default 30s
	idleTimeout  time.Duration // default 120s

	// TLS / mTLS
	tlsEnabled        bool
	tlsCertFile       string
	tlsKeyFile        string
	tlsClientCAFile   string
	requireClientCert bool

	// Path overrides (optional; default to /v1/{traces,metrics,logs})
	pathTraces  string
	pathMetrics string
	pathLogs    string
}

// New constructs an OTLP/HTTP receiver.
// Supported extras in rc.Extra:
//
// Core:
//   - max_body_bytes: int (default 16*1024*1024)
//   - read_timeout_ms: int (default 30000)
//   - write_timeout_ms: int (default 30000)
//   - idle_timeout_ms: int (default 120000)
//
// Paths (optional):
//   - paths.traces: string (default "/v1/traces")
//   - paths.metrics: string (default "/v1/metrics")
//   - paths.logs: string (default "/v1/logs")
//
// TLS:
//   - tls.enabled: bool (default false)
//   - tls.cert_file: string
//   - tls.key_file: string
//   - tls.client_ca_file: string (enables mTLS if provided)
//   - tls.require_client_cert: bool (default false)
func New(rc config.ReceiverCfg) *Receiver {
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

	// Paths
	pTr := "/v1/traces"
	pMe := "/v1/metrics"
	pLo := "/v1/logs"
	if s := nestedString(rc.Extra, "paths", "traces"); s != "" {
		pTr = s
	}
	if s := nestedString(rc.Extra, "paths", "metrics"); s != "" {
		pMe = s
	}
	if s := nestedString(rc.Extra, "paths", "logs"); s != "" {
		pLo = s
	}

	// TLS
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
		endpoint:          rc.Endpoint, // e.g. ":4318"
		maxBodyBytes:      maxBody,
		readTimeout:       rt,
		writeTimeout:      wt,
		idleTimeout:       it,
		tlsEnabled:        tlsEnabled,
		tlsCertFile:       certFile,
		tlsKeyFile:        keyFile,
		tlsClientCAFile:   caFile,
		requireClientCert: requireClientCert,
		pathTraces:        pTr,
		pathMetrics:       pMe,
		pathLogs:          pLo,
	}
}

// Start launches the HTTP server and forwards each request body as a model.Envelope.
// Shutdown is graceful on ctx cancel.
func (r *Receiver) Start(ctx context.Context, out chan<- model.Envelope) error {
	addr := r.endpoint
	if strings.TrimSpace(addr) == "" {
		addr = ":4318"
	}

	mux := http.NewServeMux()
	// Health probe (optional)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	// OTLP endpoints
	mux.HandleFunc(r.pathTraces, func(w http.ResponseWriter, req *http.Request) {
		r.handleOTLP(w, req, out, model.KindTraces)
	})
	mux.HandleFunc(r.pathMetrics, func(w http.ResponseWriter, req *http.Request) {
		r.handleOTLP(w, req, out, model.KindMetrics)
	})
	mux.HandleFunc(r.pathLogs, func(w http.ResponseWriter, req *http.Request) {
		// We pass OTLP logs through. Downstream may flatten or treat separately.
		r.handleOTLP(w, req, out, model.KindJSONLogs)
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  r.readTimeout,
		WriteTimeout: r.writeTimeout,
		IdleTimeout:  r.idleTimeout,
	}

	// Listener
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	// If TLS enabled, wrap the server with TLS config.
	if r.tlsEnabled {
		tlsCfg, err := r.buildTLS()
		if err != nil {
			return fmt.Errorf("otlphttp tls: %w", err)
		}
		srv.TLSConfig = tlsCfg
		log.Printf("[otlphttp] listening on https://%s", addr)
	} else {
		log.Printf("[otlphttp] listening on http://%s", addr)
	}

	errCh := make(chan error, 1)
	go func() {
		var serveErr error
		if r.tlsEnabled {
			serveErr = srv.ServeTLS(ln, "", "") // certs already in TLSConfig
		} else {
			serveErr = srv.Serve(ln)
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- serveErr
		}
	}()

	// Wait for stop or error
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

// handleOTLP validates method, decodes (gzip) if needed, bounds body size,
// and forwards the raw bytes as an Envelope of the given kind.
func (r *Receiver) handleOTLP(w http.ResponseWriter, req *http.Request, out chan<- model.Envelope, kind string) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Enforce size limit
	var reader io.Reader = http.MaxBytesReader(w, req.Body, r.maxBodyBytes)
	defer req.Body.Close()

	// Content-Encoding: gzip?
	if strings.Contains(strings.ToLower(req.Header.Get("Content-Encoding")), "gzip") {
		gr, err := gzip.NewReader(reader)
		if err != nil {
			http.Error(w, "invalid gzip", http.StatusBadRequest)
			return
		}
		defer gr.Close()
		reader = gr
	}

	// Read body
	body, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	// Accept protobuf or json; we forward raw either way (processors must support it)
	// Per spec, default Content-Type is application/x-protobuf.

	select {
	case out <- model.Envelope{
		Kind:   kind,
		Bytes:  body,
		Attrs:  map[string]string{}, // you can plumb auth headers here later
		TSUnix: time.Now().Unix(),
	}:
	default:
		// If channel is full, we still respond 200 to avoid backpressure on clients,
		// but drop the request (you can add internal queueing/backpressure if needed).
		log.Printf("[otlphttp] dropping request: pipeline backpressure kind=%s", kind)
	}

	// Per OTLP/HTTP, a 200 OK with empty body is a successful export.
	w.WriteHeader(http.StatusOK)
}

// buildTLS builds server TLS (and optional mTLS) config.
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
	// mTLS
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

// ----------------- tiny nested config helpers -----------------

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
