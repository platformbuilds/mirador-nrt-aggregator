package promrw

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"
)

func TestLooksSnappy(t *testing.T) {
	if looksSnappy([]byte{1, 2, 3}) {
		t.Fatalf("expected non-snappy input to be false")
	}
	encoded := snappy.Encode(nil, []byte("hello world"))
	if !looksSnappy(encoded) {
		t.Fatalf("expected snappy payload to be detected")
	}
}

func TestExtractPromHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", nil)
	req.Header.Set("X-Prometheus-Remote-Write-Version", "1")
	req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", "10")
	req.Header.Set("X-Scope-OrgID", "acme")
	req.Header.Set("Authorization", "Bearer token")

	m := extractPromHeaders(req)
	if m["promrw.version"] != "1" || m["promrw.scrape_timeout_s"] != "10" || m["org_id"] != "acme" || m["authorization"] != "Bearer token" {
		t.Fatalf("unexpected headers: %#v", m)
	}
}

func TestNestedHelpers(t *testing.T) {
	extra := map[string]any{
		"tls":   map[string]any{"enabled": true},
		"paths": map[string]any{"scope": "/x"},
	}
	if val, ok := nestedBool(extra, "tls", "enabled"); !ok || !val {
		t.Fatalf("nestedBool failed")
	}
	if got := nestedString(extra, "paths", "scope"); got != "/x" {
		t.Fatalf("nestedString failed: %q", got)
	}
	if min(2, 3) != 2 || min(5, -1) != -1 {
		t.Fatalf("min helper incorrect")
	}
}
