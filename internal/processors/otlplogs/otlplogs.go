package otlplogs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"

	colllog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	com "go.opentelemetry.io/proto/otlp/common/v1"
	res "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// processor flattens OTLP LogRecords into JSON events that downstream (logsum, filter, vectorizer) already support.
//
// Config (processors.otlplogs):
//
//	extra.resource_attrs: true         # include resource attributes  (default: true)
//	extra.scope_attrs:     true         # include scope/instrumentation attrs (name/version) (default: true)
//	extra.attr_prefix:     ""           # optional prefix for attributes keys (e.g., "attr.")
//	extra.resource_prefix: "resource."  # optional prefix for resource attributes (default: "resource.")
//	extra.scope_prefix:    "scope."     # prefix for scope fields (default: "scope.")
//	extra.level_alias:     "level"      # add duplicate field (severityText) under this key (default: "level")
//	extra.service_key:     "service.name" # which resource attr to copy as top-level service key (default "service.name")
type processor struct {
	includeRes   bool
	includeScope bool
	attrPrefix   string
	resPrefix    string
	scopePrefix  string
	levelAlias   string
	serviceKey   string
}

func New(cfg config.ProcessorCfg) *processor {
	inRes := true
	if v, ok := cfg.Extra["resource_attrs"].(bool); ok {
		inRes = v
	}
	inScope := true
	if v, ok := cfg.Extra["scope_attrs"].(bool); ok {
		inScope = v
	}
	attrPrefix := ""
	if s, ok := cfg.Extra["attr_prefix"].(string); ok {
		attrPrefix = s
	}
	resPrefix := "resource."
	if s, ok := cfg.Extra["resource_prefix"].(string); ok && s != "" {
		resPrefix = s
	}
	scopePrefix := "scope."
	if s, ok := cfg.Extra["scope_prefix"].(string); ok && s != "" {
		scopePrefix = s
	}
	levelAlias := "level"
	if s, ok := cfg.Extra["level_alias"].(string); ok && s != "" {
		levelAlias = s
	}
	serviceKey := "service.name"
	if s, ok := cfg.Extra["service_key"].(string); ok && s != "" {
		serviceKey = s
	}

	return &processor{
		includeRes:   inRes,
		includeScope: inScope,
		attrPrefix:   attrPrefix,
		resPrefix:    resPrefix,
		scopePrefix:  scopePrefix,
		levelAlias:   levelAlias,
		serviceKey:   serviceKey,
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

			env, ok := v.(model.Envelope)
			if !ok || env.Kind != model.KindJSONLogs {
				// Not an OTLP logs envelope; pass through
				out <- v
				continue
			}

			// Try to parse as OTLP ExportLogsServiceRequest; if it fails, treat as raw JSON and forward once.
			var lr colllog.ExportLogsServiceRequest
			if err := proto.Unmarshal(env.Bytes, &lr); err != nil {
				// Probably already JSON (our jsonlogs receivers), just forward
				out <- env
				continue
			}

			p.flattenAndEmit(ctx, &lr, env, out)
		}
	}
}

func (p *processor) flattenAndEmit(ctx context.Context, req *colllog.ExportLogsServiceRequest, src model.Envelope, out chan<- any) {
	now := time.Now().Unix()

	for _, rl := range req.ResourceLogs {
		rattrs := attrsToMapRes(rl.GetResource())

		for _, sl := range rl.ScopeLogs {
			scopeName, scopeVer := "", ""
			if sl.GetScope() != nil {
				scopeName = sl.GetScope().GetName()
				scopeVer = sl.GetScope().GetVersion()
			}

			for _, rec := range sl.LogRecords {
				obj := make(map[string]any, 16)

				// Timestamps
				if ts := rec.GetTimeUnixNano(); ts != 0 {
					obj["timestamp_unix_nano"] = ts
				}
				if ots := rec.GetObservedTimeUnixNano(); ots != 0 {
					obj["observed_unix_nano"] = ots
				}

				// Severity
				if txt := rec.GetSeverityText(); txt != "" {
					obj["severity_text"] = txt
					if p.levelAlias != "" {
						obj[p.levelAlias] = strings.ToLower(txt)
					}
				}
				if num := rec.GetSeverityNumber(); num != 0 {
					obj["severity_number"] = num
				}

				// Body
				if b := rec.GetBody(); b != nil {
					obj["body"] = anyVal(b)
				}

				// Trace/span
				if tr := rec.GetTraceId(); len(tr) > 0 {
					obj["trace_id"] = hexLower(tr)
				}
				if sp := rec.GetSpanId(); len(sp) > 0 {
					obj["span_id"] = hexLower(sp)
				}
				if fl := rec.GetFlags(); fl != 0 {
					obj["flags"] = fl
				}

				// Attributes
				for k, v := range attrsToMapAny(rec.Attributes) {
					obj[p.attrPrefix+k] = v
				}

				// Resource attrs
				if p.includeRes && len(rattrs) > 0 {
					for k, v := range rattrs {
						obj[p.resPrefix+k] = v
					}
					// Copy service.name as top-level "service" for our downstream defaults
					if svc, ok := rattrs[p.serviceKey]; ok && svc != "" {
						obj["service"] = svc
						obj["service.name"] = svc
					}
				}

				// Scope info
				if p.includeScope && (scopeName != "" || scopeVer != "") {
					if scopeName != "" {
						obj[p.scopePrefix+"name"] = scopeName
					}
					if scopeVer != "" {
						obj[p.scopePrefix+"version"] = scopeVer
					}
				}

				// Marshal one JSON object per record and emit
				b, err := json.Marshal(obj)
				if err != nil {
					// Fail-open: skip bad record
					log.Printf("[otlplogs] json marshal error: %v", err)
					continue
				}
				select {
				case out <- model.Envelope{
					Kind:   model.KindJSONLogs,
					Bytes:  b,
					Attrs:  src.Attrs, // preserve transport attrs if any
					TSUnix: now,
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// ---------- helpers ----------

func attrsToMapRes(r *res.Resource) map[string]string {
	out := map[string]string{}
	if r == nil {
		return out
	}
	for _, a := range r.Attributes {
		k := a.Key
		v := anyVal(a.Value)
		// Only copy scalar-ish as string for resource attrs
		switch t := v.(type) {
		case string:
			out[k] = t
		case float64:
			out[k] = trimFloat(t)
		case bool:
			if t {
				out[k] = "true"
			} else {
				out[k] = "false"
			}
		default:
			// skip complex values in resource attrs for safety
		}
	}
	return out
}

func attrsToMapAny(xs []*com.KeyValue) map[string]any {
	out := map[string]any{}
	for _, a := range xs {
		out[a.Key] = anyVal(a.Value)
	}
	return out
}

func anyVal(v *com.AnyValue) any {
	if v == nil {
		return nil
	}
	switch vv := v.Value.(type) {
	case *com.AnyValue_StringValue:
		return vv.StringValue
	case *com.AnyValue_BoolValue:
		return vv.BoolValue
	case *com.AnyValue_IntValue:
		return float64(vv.IntValue) // unify numeric to float64 for JSON
	case *com.AnyValue_DoubleValue:
		return vv.DoubleValue
	case *com.AnyValue_BytesValue:
		// represent as hex string
		return strings.ToLower(hex.EncodeToString(vv.BytesValue))
	case *com.AnyValue_ArrayValue:
		arr := vv.ArrayValue.GetValues()
		out := make([]any, 0, len(arr))
		for _, it := range arr {
			out = append(out, anyVal(it))
		}
		return out
	case *com.AnyValue_KvlistValue:
		m := map[string]any{}
		for _, kv := range vv.KvlistValue.GetValues() {
			m[kv.Key] = anyVal(kv.Value)
		}
		return m
	default:
		return nil
	}
}

func hexLower(b []byte) string {
	dst := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(dst, b)
	return strings.ToLower(string(dst))
}

func trimFloat(f float64) string {
	// compact float printing (no trailing zeros)
	s := strconv.FormatFloat(f, 'f', 6, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		return "0"
	}
	return s
}
