package weaviate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

func TestNewAppliesDefaults(t *testing.T) {
	exp := New(config.ExporterCfg{Endpoint: "http://example.com/", Class: "MyClass"})
	if exp.endpoint != "http://example.com" {
		t.Fatalf("expected endpoint without trailing slash, got %q", exp.endpoint)
	}
	if exp.class != "MyClass" {
		t.Fatalf("expected class MyClass, got %q", exp.class)
	}
	id := exp.renderID(model.Aggregate{Service: "svc", WindowStart: 123})
	if id != "svc:123" {
		t.Fatalf("expected default id template, got %q", id)
	}
}

func TestUpsertSuccess(t *testing.T) {
	var received atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/v1/objects" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("expected valid payload: %v", err)
		}
		received.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp := New(config.ExporterCfg{Endpoint: srv.URL, Class: "C", IDTemplate: "{{.Service}}-{{.Count}}"})
	exp.client = srv.Client()

	agg := model.Aggregate{Service: "svc", Count: 7}
	if err := exp.upsert(context.Background(), agg); err != nil {
		t.Fatalf("upsert returned error: %v", err)
	}
	if !received.Load() {
		t.Fatal("server did not receive request")
	}
}

func TestUpsertHandles409(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	exp := New(config.ExporterCfg{Endpoint: srv.URL, Class: "C"})
	exp.client = srv.Client()

	if err := exp.upsert(context.Background(), model.Aggregate{}); err != nil {
		t.Fatalf("expected nil error on conflict, got %v", err)
	}
}

func TestUpsertPropagatesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	exp := New(config.ExporterCfg{Endpoint: srv.URL, Class: "C"})
	exp.client = srv.Client()

	if err := exp.upsert(context.Background(), model.Aggregate{}); err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestStartStopsOnContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp := New(config.ExporterCfg{Endpoint: srv.URL, Class: "C"})
	exp.client = srv.Client()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan model.Aggregate, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := exp.Start(ctx, ch); err != nil {
			t.Errorf("start returned error: %v", err)
		}
	}()

	ch <- model.Aggregate{}
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("start did not return after cancel")
	}
}
