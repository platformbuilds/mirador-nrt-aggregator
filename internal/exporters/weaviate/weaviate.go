package weaviate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"text/template"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

type Exporter struct {
	endpoint   string
	class      string
	idTemplate *template.Template
	client     *http.Client
}

// New creates a new Weaviate exporter from config.
func New(cfg config.ExporterCfg) *Exporter {
	tmpl := "{{.Service}}:{{.WindowStart}}"
	if cfg.IDTemplate != "" {
		tmpl = cfg.IDTemplate
	}
	tt, err := template.New("id").Parse(tmpl)
	if err != nil {
		log.Fatalf("weaviate exporter: invalid id_template: %v", err)
	}
	return &Exporter{
		endpoint:   strings.TrimSuffix(cfg.Endpoint, "/"),
		class:      cfg.Class,
		idTemplate: tt,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// Start runs the exporter, consuming Aggregates until the input channel closes.
func (e *Exporter) Start(ctx context.Context, in <-chan model.Aggregate) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case a, ok := <-in:
			if !ok {
				return nil
			}
			if err := e.upsert(ctx, a); err != nil {
				log.Printf("weaviate exporter: upsert failed: %v", err)
			}
		}
	}
}

// upsert builds a Weaviate object and sends it to /v1/objects.
func (e *Exporter) upsert(ctx context.Context, a model.Aggregate) error {
	id := e.renderID(a)
	body := map[string]any{
		"class":  e.class,
		"id":     id,
		"vector": a.Vector,
		"properties": map[string]any{
			"summary":       a.SummaryText,
			"service":       a.Service,
			"window_start":  a.WindowStart,
			"window_end":    a.WindowEnd,
			"p50":           a.P50,
			"p95":           a.P95,
			"p99":           a.P99,
			"rps":           a.RPS,
			"error_rate":    a.ErrorRate,
			"anomaly_score": a.AnomalyScore,
			"count":         a.Count,
			"labels":        a.Labels,
			"locator":       a.Locator,
		},
	}
	b, _ := json.Marshal(body)

	req, _ := http.NewRequestWithContext(ctx, "POST", e.endpoint+"/v1/objects", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	// Handle already exists (409) gracefully.
	if resp.StatusCode == 409 {
		// Could add PATCH support here.
		return nil
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("weaviate HTTP %d", resp.StatusCode)
	}
	return nil
}

func (e *Exporter) renderID(a model.Aggregate) string {
	var sb strings.Builder
	if err := e.idTemplate.Execute(&sb, a); err != nil {
		// fallback
		return fmt.Sprintf("%s:%d", a.Service, a.WindowStart)
	}
	return sb.String()
}
