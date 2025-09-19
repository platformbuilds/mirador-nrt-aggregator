package filter

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/google/cel-go/cel"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

type processor struct {
	stage           string // "pre" or "post"
	on              string // "metrics" | "logs" | "traces" | "aggregates"
	dropNonMatching bool

	expr string
	env  *cel.Env
	prg  cel.Program
}

// New builds a CEL program from the provided configuration.
//
// Example config snippet:
// processors:
//
//	filter/metrics-pre:
//	  stage: pre
//	  on: metrics
//	  drop_non_matching: true
//	  expr: 'attrs["env"] == "prod"'
//
//	filter/agg-post:
//	  stage: post
//	  on: aggregates
//	  drop_non_matching: true
//	  expr: 'anomaly_score >= 0.8 || error_rate > 0.05'
func New(cfg config.ProcessorCfg) *processor {
	stage := strings.ToLower(cfg.ExtraString("stage", "pre"))
	on := strings.ToLower(cfg.ExtraString("on", "metrics"))
	expr := cfg.ExtraString("expr", "true")
	drop := cfg.ExtraBool("drop_non_matching", true)

	// Build a CEL environment. We keep declarations very permissive (DynType)
	// so users can access arbitrary fields without tight typing.
	decls := []cel.EnvOption{
		cel.Variable("now_unix", cel.IntType),
	}

	switch on {
	case "metrics":
		// Raw envelope variables available for metrics:
		// kind, ts_unix, attrs (map), raw is not exposed
		decls = append(decls,
			cel.Variable("kind", cel.StringType),
			cel.Variable("ts_unix", cel.IntType),
			cel.Variable("attrs", cel.DynType),
			// Optional placeholders (often empty unless a previous proc filled them):
			cel.Variable("metric_name", cel.StringType),
			cel.Variable("labels", cel.DynType),
			cel.Variable("resource", cel.DynType),
		)
	case "logs":
		// Every top-level JSON field becomes part of 'log' map
		decls = append(decls,
			cel.Variable("ts_unix", cel.IntType),
			cel.Variable("log", cel.DynType),
		)
	case "traces":
		decls = append(decls,
			cel.Variable("kind", cel.StringType),
			cel.Variable("ts_unix", cel.IntType),
			cel.Variable("attributes", cel.DynType),
			cel.Variable("resource", cel.DynType),
			cel.Variable("span_name", cel.StringType),
			cel.Variable("duration_ms", cel.DoubleType),
		)
	case "aggregates":
		decls = append(decls,
			cel.Variable("service", cel.StringType),
			cel.Variable("window_start", cel.IntType),
			cel.Variable("window_end", cel.IntType),
			cel.Variable("p50", cel.DoubleType),
			cel.Variable("p95", cel.DoubleType),
			cel.Variable("p99", cel.DoubleType),
			cel.Variable("rps", cel.DoubleType),
			cel.Variable("error_rate", cel.DoubleType),
			cel.Variable("anomaly_score", cel.DoubleType),
			cel.Variable("count", cel.DoubleType),
			cel.Variable("labels", cel.DynType),
			cel.Variable("summary", cel.StringType),
		)
	default:
		// Fallback to a generic environment to avoid breaking unknown configs.
		decls = append(decls, cel.Variable("item", cel.DynType))
	}

	env, err := cel.NewEnv(decls...)
	if err != nil {
		log.Printf("[filter] cel env init error: %v; defaulting to pass-through", err)
		// Create a minimal env to allow compiling "true"
		env, _ = cel.NewEnv()
		expr = "true"
	}

	ast, iss := env.Parse(expr)
	if iss != nil && iss.Err() != nil {
		log.Printf("[filter] cel parse error for expr %q: %v; defaulting to pass-through", expr, iss.Err())
		ast, _ = env.Parse("true")
	}
	checked, iss := env.Check(ast)
	if iss != nil && iss.Err() != nil {
		log.Printf("[filter] cel type-check error for expr %q: %v; defaulting to pass-through", expr, iss.Err())
		checked = ast // fall back to un-checked ast
	}
	prg, err := env.Program(checked)
	if err != nil {
		log.Printf("[filter] cel program error: %v; defaulting to pass-through", err)
		// last resort: program for constant true
		astTrue, _ := env.Parse("true")
		prg, _ = env.Program(astTrue)
	}

	return &processor{
		stage:           stage,
		on:              on,
		dropNonMatching: drop,
		expr:            expr,
		env:             env,
		prg:             prg,
	}
}

func (p *processor) Start(ctx context.Context, in <-chan any, out chan<- any) error {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return nil
		case v, ok := <-in:
			if !ok {
				return nil
			}
			switch t := v.(type) {
			case model.Envelope:
				if p.stage != "pre" {
					// Not interested in pre stage, pass through.
					out <- t
					continue
				}
				keep := p.evalEnvelope(t)
				if keep || !p.dropNonMatching {
					out <- t
				}
			case model.Aggregate:
				if p.stage != "post" && p.on != "aggregates" {
					out <- t
					continue
				}
				keep := p.evalAggregate(t)
				if keep || !p.dropNonMatching {
					out <- t
				}
			default:
				// Unknown type -> pass through.
				out <- v
			}
		}
	}
}

// ---------------- evaluation helpers ----------------

func (p *processor) evalEnvelope(e model.Envelope) bool {
	// Always expose time.
	act := map[string]any{
		"now_unix": time.Now().Unix(),
		"ts_unix":  e.TSUnix,
	}

	switch p.on {
	case "metrics":
		act["kind"] = e.Kind
		act["attrs"] = e.Attrs
		// Placeholders (unless some previous component enriched the envelope):
		act["metric_name"] = ""          // could be populated by a lightweight name-extractor earlier
		act["labels"] = map[string]any{} // ditto
		act["resource"] = map[string]any{}
	case "logs":
		// Parse a single JSON log line into a generic map exposed as 'log'.
		var m map[string]any
		if err := json.Unmarshal(e.Bytes, &m); err == nil {
			act["log"] = m
		} else {
			// If invalid JSON, pass-through (fail-open)
			act["log"] = map[string]any{}
		}
	case "traces":
		// Without full OTLP decode here, we provide generic shells.
		act["kind"] = e.Kind
		act["attributes"] = map[string]any{}
		act["resource"] = map[string]any{}
		act["span_name"] = ""
		act["duration_ms"] = float64(0)
	default:
		// Generic item
		act["item"] = map[string]any{
			"kind":    e.Kind,
			"ts_unix": e.TSUnix,
			"attrs":   e.Attrs,
		}
	}

	return p.eval(act)
}

func (p *processor) evalAggregate(a model.Aggregate) bool {
	act := map[string]any{
		"now_unix":      time.Now().Unix(),
		"service":       a.Service,
		"window_start":  a.WindowStart,
		"window_end":    a.WindowEnd,
		"p50":           a.P50,
		"p95":           a.P95,
		"p99":           a.P99,
		"rps":           a.RPS,
		"error_rate":    a.ErrorRate,
		"anomaly_score": a.AnomalyScore,
		"count":         float64(a.Count),
		"labels":        a.Labels,
		"summary":       a.SummaryText,
	}
	return p.eval(act)
}

func (p *processor) eval(vars map[string]any) bool {
	// Fail-open: if CEL eval errors or does not return a bool, keep the item.
	out, _, err := p.prg.Eval(vars)
	if err != nil {
		// Useful for debugging: uncomment to see expression failures.
		// log.Printf("[filter] eval error for expr=%q vars=%v: %v", p.expr, vars, err)
		return true
	}
	if b, ok := out.Value().(bool); ok {
		return b
	}
	return true
}
