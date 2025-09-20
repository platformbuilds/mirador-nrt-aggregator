package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
)

func TestEnvOr(t *testing.T) {
	os.Setenv("TEST_KEY", "value")
	t.Cleanup(func() { os.Unsetenv("TEST_KEY") })

	if got := envOr("TEST_KEY", "default"); got != "value" {
		t.Fatalf("expected env value, got %q", got)
	}
	if got := envOr("MISSING", "default"); got != "default" {
		t.Fatalf("expected default, got %q", got)
	}
}

func TestSetupMetricsMux(t *testing.T) {
	ready := &atomic.Bool{}
	handler := setupMetricsMux(ready)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/livez", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("livez expected 200, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/readyz", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != 503 {
		t.Fatalf("readyz expected 503 when not ready, got %d", rr.Code)
	}

	ready.Store(true)
	rr = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/readyz", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("readyz expected 200 when ready, got %d", rr.Code)
	}
}

func TestRegisterPprofPanics(t *testing.T) {
	mux := http.NewServeMux()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected registerPprof to panic for default stub")
		}
	}()
	registerPprof(mux)
}
