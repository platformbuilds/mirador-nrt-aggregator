package jsonlogs

import (
	"context"
	"net"
	"net/http"
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
	addr := freePort(t)
	rec := NewHTTP(config.ReceiverCfg{Endpoint: addr})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan model.Envelope, 1)
	done := make(chan error, 1)
	go func() { done <- rec.Start(ctx, out) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("start returned error: %v", err)
		}
	}()

	waitForListener(t, addr)

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+defaultPath, strings.NewReader("not gzip"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http post: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad gzip, got %d", resp.StatusCode)
	}

	select {
	case env := <-out:
		t.Fatalf("unexpected envelope produced: %+v", env)
	default:
	}
}

func TestHTTPReceiverBackpressureBlocksUntilDrained(t *testing.T) {
	addr := freePort(t)
	rec := NewHTTP(config.ReceiverCfg{Endpoint: addr})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan model.Envelope)
	done := make(chan error, 1)
	go func() { done <- rec.Start(ctx, out) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("start returned error: %v", err)
		}
	}()

	waitForListener(t, addr)

	body := "{\"msg\":1}\n{\"msg\":2}\n"

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+defaultPath, strings.NewReader(body))
		if err != nil {
			errCh <- err
			return
		}
		req.Header.Set("Content-Type", "application/x-ndjson")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	select {
	case <-respCh:
		t.Fatal("request should block until envelopes drained")
	case err := <-errCh:
		t.Fatalf("http post failed early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	first := <-out
	second := <-out
	if !strings.Contains(string(first.Bytes), "msg") || !strings.Contains(string(second.Bytes), "msg") {
		t.Fatalf("expected log payloads, got %q and %q", first.Bytes, second.Bytes)
	}

	select {
	case resp := <-respCh:
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", resp.StatusCode)
		}
	case err := <-errCh:
		t.Fatalf("http post failed: %v", err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("request did not complete after draining")
	}
}

func freePort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func waitForListener(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for listener on %s", addr)
}
