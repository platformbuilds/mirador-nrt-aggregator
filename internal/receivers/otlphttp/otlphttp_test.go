package otlphttp

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

func TestHandleOTLPPlainBody(t *testing.T) {
	r := New(config.ReceiverCfg{})
	req := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader([]byte("raw")))
	w := httptest.NewRecorder()
	ch := make(chan model.Envelope, 1)

	r.handleOTLP(w, req, ch, model.KindMetrics)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Result().StatusCode)
	}
	select {
	case env := <-ch:
		if env.Kind != model.KindMetrics {
			t.Fatalf("unexpected kind %s", env.Kind)
		}
		if string(env.Bytes) != "raw" {
			t.Fatalf("unexpected bytes %q", string(env.Bytes))
		}
	default:
		t.Fatalf("expected envelope to be forwarded")
	}
}

func TestHandleOTLPGzip(t *testing.T) {
	r := New(config.ReceiverCfg{})

	buf := new(bytes.Buffer)
	gz := gzip.NewWriter(buf)
	if _, err := gz.Write([]byte("compressed")); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	ch := make(chan model.Envelope, 1)

	r.handleOTLP(w, req, ch, model.KindJSONLogs)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Result().StatusCode)
	}
	env := <-ch
	if env.Kind != model.KindJSONLogs {
		t.Fatalf("unexpected kind %s", env.Kind)
	}
	if string(env.Bytes) != "compressed" {
		t.Fatalf("expected decompressed bytes, got %q", string(env.Bytes))
	}
}

func TestNestedHelpers(t *testing.T) {
	extra := map[string]any{
		"paths": map[string]any{
			"traces": "/custom",
		},
		"tls": map[string]any{
			"enabled": true,
		},
	}
	if got := nestedString(extra, "paths", "traces"); got != "/custom" {
		t.Fatalf("nestedString mismatch %q", got)
	}
	if val, ok := nestedBool(extra, "tls", "enabled"); !ok || !val {
		t.Fatalf("nestedBool mismatch")
	}
}
