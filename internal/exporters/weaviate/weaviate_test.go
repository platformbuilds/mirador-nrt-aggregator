package weaviate

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

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
	var recorded atomic.Value
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
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
		recorded.Store(payload)
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
	})

	exp := New(config.ExporterCfg{Endpoint: "http://example.com", Class: "C", IDTemplate: "{{.Service}}-{{.Count}}"})
	exp.client = &http.Client{Transport: transport}

	agg := model.Aggregate{Service: "svc", Count: 7}
	if err := exp.upsert(context.Background(), agg); err != nil {
		t.Fatalf("upsert returned error: %v", err)
	}
	if recorded.Load() == nil {
		t.Fatal("request payload not captured")
	}
}

func TestUpsertHandles409(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusConflict, Body: http.NoBody, Header: make(http.Header)}, nil
	})

	exp := New(config.ExporterCfg{Endpoint: "http://example.com", Class: "C"})
	exp.client = &http.Client{Transport: transport}

	if err := exp.upsert(context.Background(), model.Aggregate{}); err != nil {
		t.Fatalf("expected nil error on conflict, got %v", err)
	}
}

func TestUpsertPropagatesHTTPError(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusInternalServerError, Body: http.NoBody, Header: make(http.Header)}, nil
	})

	exp := New(config.ExporterCfg{Endpoint: "http://example.com", Class: "C"})
	exp.client = &http.Client{Transport: transport}

	if err := exp.upsert(context.Background(), model.Aggregate{}); err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestUpsertPropagatesTransportErrors(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("boom")
	})

	exp := New(config.ExporterCfg{Endpoint: "http://example.com", Class: "C"})
	exp.client = &http.Client{Transport: transport}

	if err := exp.upsert(context.Background(), model.Aggregate{}); err == nil {
		t.Fatal("expected transport error to propagate")
	}
}

func TestStartStopsOnContextCancel(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
	})

	exp := New(config.ExporterCfg{Endpoint: "http://example.com", Class: "C"})
	exp.client = &http.Client{Transport: transport}

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
