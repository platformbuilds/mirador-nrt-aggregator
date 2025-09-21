package jsonlogs

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

func TestNewHTTPDefaultsAndOverrides(t *testing.T) {
	r := NewHTTP(config.ReceiverCfg{})
	if r.path != defaultPath {
		t.Fatalf("expected default path %q, got %q", defaultPath, r.path)
	}

	custom := NewHTTP(config.ReceiverCfg{Endpoint: "0.0.0.0:9999", Extra: map[string]any{"path": "/custom"}})
	if custom.addr != "0.0.0.0:9999" {
		t.Fatalf("endpoint not preserved, got %q", custom.addr)
	}
	if custom.path != "/custom" {
		t.Fatalf("expected custom path, got %q", custom.path)
	}
}

func TestKafkaStartValidatesConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &KafkaReceiver{}
	err := r.Start(ctx, make(chan model.Envelope))
	if err != ErrKafkaConfig {
		t.Fatalf("expected ErrKafkaConfig, got %v", err)
	}
}

func TestGroupOrDefault(t *testing.T) {
	r := &KafkaReceiver{}
	if got := r.groupOrDefault(); got != "mirador-jsonlogs" {
		t.Fatalf("expected fallback group, got %q", got)
	}
	r.group = "team"
	if got := r.groupOrDefault(); got != "team" {
		t.Fatalf("expected explicit group, got %q", got)
	}
}

func TestHeadersToMap(t *testing.T) {
	hdrs := []kafka.Header{{Key: "k1", Value: []byte("v1")}, {Key: "k2", Value: []byte("v2")}}
	m := headersToMap(hdrs)
	if len(m) != 2 || m["k1"] != "v1" || m["k2"] != "v2" {
		t.Fatalf("unexpected map: %#v", m)
	}
	if len(headersToMap(nil)) != 0 {
		t.Fatalf("expected empty map for nil headers")
	}
}

func TestHTTPReceiverStartInvalidAddress(t *testing.T) {
	rec := &HTTPReceiver{addr: "127.0.0.1:notaport", path: defaultPath}
	if err := rec.Start(context.Background(), make(chan model.Envelope)); err == nil {
		t.Fatalf("expected error for invalid listen address")
	}
}

func TestHTTPReceiverMalformedGzipReturns400(t *testing.T) {
	rec := NewHTTP(config.ReceiverCfg{})
	out := make(chan model.Envelope, 1)
	handler := rec.handler(out)

	req := httptest.NewRequest(http.MethodPost, "http://example.com"+defaultPath, strings.NewReader("not gzip"))
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad gzip, got %d", rr.Code)
	}

	select {
	case env := <-out:
		t.Fatalf("unexpected envelope produced: %+v", env)
	default:
	}
}

func TestHTTPReceiverBackpressureBlocksUntilDrained(t *testing.T) {
	rec := NewHTTP(config.ReceiverCfg{})
	out := make(chan model.Envelope)
	handler := rec.handler(out)

	body := "{\"msg\":1}\n{\"msg\":2}\n"
	req := httptest.NewRequest(http.MethodPost, "http://example.com"+defaultPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-ndjson")

	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rr, req)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("handler should block until envelopes drained")
	case <-time.After(100 * time.Millisecond):
	}

	first := <-out
	second := <-out
	if !strings.Contains(string(first.Bytes), "msg") || !strings.Contains(string(second.Bytes), "msg") {
		t.Fatalf("expected log payloads, got %q and %q", first.Bytes, second.Bytes)
	}

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler did not finish after draining")
	}

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr.Code)
	}
}

func TestHandlerParsesGzip(t *testing.T) {
	rec := NewHTTP(config.ReceiverCfg{})
	out := make(chan model.Envelope, 1)
	handler := rec.handler(out)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte("{\"msg\":1}"))
	_ = gz.Close()

	req := httptest.NewRequest(http.MethodPost, "http://example.com"+defaultPath, bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr.Code)
	}
	select {
	case <-out:
	case <-time.After(time.Second):
		t.Fatal("expected envelope from gzip payload")
	}
}

func TestHandlerSplitsNDJSONWithBufferGrowth(t *testing.T) {
	rec := NewHTTP(config.ReceiverCfg{})
	out := make(chan model.Envelope, 2)
	handler := rec.handler(out)

	var long bytes.Buffer
	writer := bufio.NewWriter(&long)
	_, _ = writer.WriteString(strings.Repeat("a", 70*1024) + "\n")
	_, _ = writer.WriteString("{\"msg\":2}\n")
	writer.Flush()

	req := httptest.NewRequest(http.MethodPost, "http://example.com"+defaultPath, bytes.NewReader(long.Bytes()))
	req.Header.Set("Content-Type", "application/x-ndjson")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr.Code)
	}

	if len(out) != 2 {
		t.Fatalf("expected 2 envelopes, got %d", len(out))
	}
}
