package stdout

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

type Exporter struct {
	logger        *log.Logger
	pretty        bool
	includeVector bool
}

func New(cfg config.ExporterCfg) *Exporter {
	pretty := extraBool(cfg.Extra, "pretty", false)
	includeVector := extraBool(cfg.Extra, "include_vector", true)

	return &Exporter{
		logger:        log.New(os.Stdout, "[stdout-exporter] ", log.LstdFlags),
		pretty:        pretty,
		includeVector: includeVector,
	}
}

func (e *Exporter) Start(ctx context.Context, in <-chan model.Aggregate) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case agg, ok := <-in:
			if !ok {
				return nil
			}
			e.printAggregate(agg)
		}
	}
}

func (e *Exporter) printAggregate(a model.Aggregate) {
	payload := map[string]any{
		"service":       a.Service,
		"window_start":  a.WindowStart,
		"window_end":    a.WindowEnd,
		"labels":        a.Labels,
		"locator":       a.Locator,
		"count":         a.Count,
		"rps":           a.RPS,
		"error_rate":    a.ErrorRate,
		"p50":           a.P50,
		"p95":           a.P95,
		"p99":           a.P99,
		"anomaly_score": a.AnomalyScore,
		"summary":       a.SummaryText,
	}

	if e.includeVector && len(a.Vector) > 0 {
		payload["vector"] = a.Vector
	}

	b, err := e.marshal(payload)
	if err != nil {
		e.logger.Printf("marshal failed: %v", err)
		return
	}

	e.logger.Printf("%s", b)
}

func (e *Exporter) marshal(payload map[string]any) ([]byte, error) {
	if e.pretty {
		return json.MarshalIndent(payload, "", "  ")
	}
	return json.Marshal(payload)
}

func extraBool(m map[string]any, key string, def bool) bool {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case bool:
			return vv
		case string:
			if vv == "true" {
				return true
			}
			if vv == "false" {
				return false
			}
		}
	}
	return def
}
