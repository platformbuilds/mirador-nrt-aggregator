package vectorizer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yourorg/mirador-nrt-aggregator/internal/config"
	"github.com/yourorg/mirador-nrt-aggregator/internal/model"
)

// -------- Config model --------
//
// processors:
//   vectorizer:
//     # global text backend for logs/traces: "ollama" | "hash"
//     mode: "ollama"
//     endpoint: "http://localhost:11434"
//     model: "nomic-embed-text"
//     allow_fallback: true
//     timeout_ms: 5000
//     retries: 2
//
//     # hashing fallback/options (CPU-only embedding):
//     hash_dim: 384
//     hash_ngrams: 2
//     lowercase: true
//     stopwords: ["the","a","an","and","or","to","of","in","on","for","with"]
//
//     # Branching behavior per signal type (detected via Aggregate.Labels["source"] == logs|metrics|traces)
//     logs:
//       include_fields: ["org_id","success","error_code"]   # will be spliced into the text context if present
//     metrics:
//       # build numerical features -> (optional) PCA projection
//       p90_approx: true            # use (p50+p95)/2 as p90 approximation
//       ema_alpha: 0.3              # optional EMA smoothing across windows (per service)
//       pca:
//         enabled: true
//         # Either inline matrix or a path to a JSON file with fields: {"rows":M,"cols":N,"data":[...row-major...]}
//         matrix_path: "/etc/mirador/models/pca_metrics.json"
//         # Or: matrix_inline: '{"rows":8,"cols":32,"data":[...]}'
//         center: [0,0,0,0,0,0,0,0]  # mean for centering (optional)
//         scale:  [1,1,1,1,1,1,1,1]  # stdev for scaling (optional)
//     traces:
//       include_span_attrs: ["http.method","http.route","status.code","span.kind","db.system","rpc.system"]
//       max_attrs: 6

type processor struct {
	// text embedding backend for logs/traces
	mode          string // "ollama" | "hash"
	allowFallback bool

	// Ollama
	ollamaEndpoint string
	ollamaModel    string
	client         *http.Client
	retries        int

	// Hashing (CPU) options
	hashDim    int
	hashNgrams int
	lowercase  bool
	stopwords  map[string]struct{}
	tokenSplit *regexp.Regexp

	// LOGS options
	logsInclude []string // fields to splice into text (org_id, success, error_code, ...)

	// METRICS options
	p90Approx bool
	emaAlpha  float64
	pca       *pcaModel // optional
	// EMA state per service for metrics (RPS, ErrorRate, p50,p90,p95,p99,errCount,countNorm)
	ema map[string][]float64

	// TRACES options
	traceAttrs []string
	traceMax   int
}

// PCA model (row-major projection matrix)
type pcaModel struct {
	rows int
	cols int
	mat  []float64 // rows x cols, row-major
	mean []float64 // optional centering
	std  []float64 // optional scaling
}

func New(cfg config.ProcessorCfg) *processor {
	// globals
	mode := strings.ToLower(cfg.ExtraString("mode", "ollama"))
	endpoint := strings.TrimSuffix(cfg.ExtraString("endpoint", "http://localhost:11434"), "/")
	modelName := cfg.ExtraString("model", "nomic-embed-text")
	timeoutMs := 5000
	if v, ok := cfg.Extra["timeout_ms"].(int); ok && v > 0 {
		timeoutMs = v
	}
	retries := 2
	if v, ok := cfg.Extra["retries"].(int); ok {
		retries = v
	}
	allowFallback := true
	if v, ok := cfg.Extra["allow_fallback"].(bool); ok {
		allowFallback = v
	}

	// hashing
	hashDim := 384
	if v, ok := cfg.Extra["hash_dim"].(int); ok && v > 0 {
		hashDim = v
	}
	hashN := 2
	if v, ok := cfg.Extra["hash_ngrams"].(int); ok && v >= 1 && v <= 3 {
		hashN = v
	}
	lower := true
	if v, ok := cfg.Extra["lowercase"].(bool); ok {
		lower = v
	}
	stop := map[string]struct{}{}
	if arr, ok := cfg.Extra["stopwords"].([]any); ok {
		for _, it := range arr {
			if s, ok := it.(string); ok && s != "" {
				if lower {
					s = strings.ToLower(s)
				}
				stop[s] = struct{}{}
			}
		}
	} else {
		for _, s := range []string{"the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "with"} {
			stop[s] = struct{}{}
		}
	}

	// logs fields
	var logsInc []string
	if arr, ok := nestedStringSlice(cfg.Extra, "logs", "include_fields"); ok {
		logsInc = arr
	} else {
		logsInc = []string{"org_id", "success", "error_code"}
	}

	// metrics options
	p90Approx := true
	if v, ok := nestedBool(cfg.Extra, "metrics", "p90_approx"); ok {
		p90Approx = v
	}
	emaAlpha := 0.0
	if v, ok := nestedNumber(cfg.Extra, "metrics", "ema_alpha"); ok {
		emaAlpha = v
	}
	var pca *pcaModel
	if nestedBoolDefault(cfg.Extra, false, "metrics", "pca", "enabled") {
		if m, err := loadPCA(cfg.Extra["metrics"]); err == nil {
			pca = m
		} else {
			log.Printf("[vectorizer] PCA requested but not loaded: %v (continuing without PCA)", err)
		}
	}

	// traces options
	traceAttrs := []string{"http.method", "http.route", "status.code", "span.kind"}
	if arr, ok := nestedStringSlice(cfg.Extra, "traces", "include_span_attrs"); ok && len(arr) > 0 {
		traceAttrs = arr
	}
	traceMax := 6
	if v, ok := nestedInt(cfg.Extra, "traces", "max_attrs"); ok && v > 0 {
		traceMax = v
	}

	return &processor{
		mode:           mode,
		allowFallback:  allowFallback,
		ollamaEndpoint: endpoint,
		ollamaModel:    modelName,
		client:         &http.Client{Timeout: time.Duration(timeoutMs) * time.Millisecond},
		retries:        retries,

		hashDim:    hashDim,
		hashNgrams: hashN,
		lowercase:  lower,
		stopwords:  stop,
		tokenSplit: regexp.MustCompile(`[^a-zA-Z0-9]+`),

		logsInclude: logsInc,

		p90Approx: p90Approx,
		emaAlpha:  emaAlpha,
		pca:       pca,
		ema:       map[string][]float64{},

		traceAttrs: traceAttrs,
		traceMax:   traceMax,
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
			a, ok := v.(model.Aggregate)
			if !ok {
				out <- v
				continue
			}

			source := strings.ToLower(a.Labels["source"]) // expected: "logs" | "metrics" | "traces" (set by upstream)
			switch source {
			case "logs":
				a.Vector = p.embedText(ctx, p.buildLogsText(a))
			case "traces":
				a.Vector = p.embedText(ctx, p.buildTracesText(a))
			case "metrics":
				a.Vector = p.buildMetricsVector(a) // numeric vector (optionally PCA)
			default:
				// If unknown, fall back to text embedding of the summary
				txt := a.SummaryText
				if strings.TrimSpace(txt) == "" {
					txt = p.defaultSummary(a)
				}
				a.Vector = p.embedText(ctx, txt)
			}

			// forward
			out <- a
		}
	}
}

// -------------------------- LOGS --------------------------

func (p *processor) buildLogsText(a model.Aggregate) string {
	// Include org_id, success, error_code when present (and any others from config)
	parts := []string{
		"kind=log",
		"service=" + a.Service,
		"ts_start=" + strconv(a.WindowStart),
		"ts_end=" + strconv(a.WindowEnd),
	}

	// success default: success if error_rate ~ 0
	success := "true"
	if a.ErrorRate > 0.0 {
		success = "false"
	}
	// ensure success is favored even if user didn't provide one
	added := map[string]bool{"success": false}

	// deterministic order for configured fields
	fields := append([]string{}, p.logsInclude...)
	sort.Strings(fields)
	for _, k := range fields {
		v := a.Labels[k]
		if k == "success" && v == "" {
			v = success
			added["success"] = true
		}
		if v == "" {
			continue
		}
		parts = append(parts, k+"="+v)
	}
	if !added["success"] {
		parts = append(parts, "success="+success)
	}

	// Append short summary
	if a.SummaryText != "" {
		parts = append(parts, "summary="+a.SummaryText)
	}
	return strings.Join(parts, " ")
}

// -------------------------- METRICS --------------------------

func (p *processor) buildMetricsVector(a model.Aggregate) []float32 {
	// Build numeric features for service health
	// base: [p50, p90, p95, p99, rps, error_rate, error_count, count_norm]
	p90 := a.P95
	if p.p90Approx {
		p90 = 0.5*(a.P50+a.P95)
	}
	errCount := a.ErrorRate * float64(a.Count)
	countNorm := float64(a.Count) / (1.0 + float64(a.WindowEnd-a.WindowStart))

	base := []float64{
		a.P50, p90, a.P95, a.P99,
		a.RPS, a.ErrorRate, errCount, countNorm,
	}

	// Optional EMA smoothing per service
	if p.emaAlpha > 0 && p.emaAlpha <= 1 {
		prev := p.ema[a.Service]
		if len(prev) != len(base) {
			prev = make([]float64, len(base))
		}
		for i := range base {
			prev[i] = p.emaAlpha*base[i] + (1-p.emaAlpha)*prev[i]
		}
		p.ema[a.Service] = prev
		copy(base, prev)
	}

	// Optional PCA projection (center -> scale -> project)
	if p.pca != nil && p.pca.cols == len(base) && len(p.pca.mat) == p.pca.rows*p.pca.cols {
		vec := make([]float32, p.pca.rows)
		x := make([]float64, len(base))
		copy(x, base)
		// center/scale
		if len(p.pca.mean) == len(x) {
			for i := range x {
				x[i] -= p.pca.mean[i]
			}
		}
		if len(p.pca.std) == len(x) {
			for i := range x {
				if p.pca.std[i] > 1e-9 {
					x[i] /= p.pca.std[i]
				}
			}
		}
		// y = W * x
		for r := 0; r < p.pca.rows; r++ {
			sum := 0.0
			row := p.pca.mat[r*p.pca.cols : (r+1)*p.pca.cols]
			for c := 0; c < p.pca.cols; c++ {
				sum += row[c] * x[c]
			}
			vec[r] = float32(sum)
		}
		normalize(vec)
		return vec
	}

	// No PCA → return normalized base features as vector
	out := make([]float32, len(base))
	for i := range base {
		out[i] = float32(base[i])
	}
	normalize(out)
	return out
}

// -------------------------- TRACES --------------------------

func (p *processor) buildTracesText(a model.Aggregate) string {
	// Treat trace aggregates as short, structured text
	parts := []string{
		"kind=trace",
		"service=" + a.Service,
		"ts_start=" + strconv(a.WindowStart),
		"ts_end=" + strconv(a.WindowEnd),
		"p50=" + f6(a.P50),
		"p95=" + f6(a.P95),
		"p99=" + f6(a.P99),
		"rps=" + f6(a.RPS),
		"error_rate=" + f6(a.ErrorRate),
	}
	// include a few span/resource attrs if upstream added them into Labels
	// (e.g., http.method, http.route, status.code, span.kind, etc.)
	if len(p.traceAttrs) > 0 {
		added := 0
		for _, k := range p.traceAttrs {
			if v, ok := a.Labels[k]; ok && v != "" {
				parts = append(parts, k+"="+v)
				added++
				if added >= p.traceMax {
					break
				}
			}
		}
	}
	if a.SummaryText != "" {
		parts = append(parts, "summary="+a.SummaryText)
	}
	return strings.Join(parts, " ")
}

// ---------------------- Text embedding ----------------------

func (p *processor) embedText(ctx context.Context, text string) []float32 {
	switch p.mode {
	case "ollama":
		emb, err := p.embedOllama(ctx, text)
		if err == nil && len(emb) > 0 {
			return emb
		}
		if p.allowFallback {
			return p.embedHashing(text)
		}
		return nil
	case "hash":
		return p.embedHashing(text)
	default:
		// try ollama then hash
		emb, err := p.embedOllama(ctx, text)
		if err == nil && len(emb) > 0 {
			return emb
		}
		return p.embedHashing(text)
	}
}

func (p *processor) embedOllama(ctx context.Context, text string) ([]float32, error) {
	body := map[string]any{"model": p.ollamaModel, "prompt": text}
	b, _ := json.Marshal(body)
	url := p.ollamaEndpoint + "/api/embeddings"

	var lastErr error
	for attempt := 0; attempt <= p.retries; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		var out struct {
			Embedding []float32 `json:"embedding"`
		}
		func() {
			defer resp.Body.Close()
			if resp.StatusCode >= 300 {
				io.Copy(io.Discard, resp.Body)
				lastErr = errors.New(resp.Status)
				return
			}
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				lastErr = err
				return
			}
			if len(out.Embedding) == 0 {
				lastErr = errors.New("empty embedding")
				return
			}
			lastErr = nil
		}()
		if lastErr == nil {
			normalize(out.Embedding)
			return out.Embedding, nil
		}
	}
	return nil, lastErr
}

func (p *processor) embedHashing(text string) []float32 {
	toks := p.tokenize(text)
	vec := make([]float32, p.hashDim)

	emit := func(s string, weight float32) {
		if s == "" {
			return
		}
		// index
		h := fnv.New64a()
		_, _ = h.Write([]byte(s))
		idx := int(h.Sum64() % uint64(p.hashDim))
		// sign
		hs := fnv.New64()
		_, _ = hs.Write([]byte("sign:" + s))
		sign := int(hs.Sum64() & 1)
		val := weight
		if sign == 1 {
			val = -val
		}
		vec[idx] += val
	}

	// unigrams
	for _, t := range toks {
		if _, stop := p.stopwords[t]; stop {
			continue
		}
		emit(t, 1)
	}
	// n-grams
	if p.hashNgrams >= 2 {
		for i := 0; i+1 < len(toks); i++ {
			if _, s1 := p.stopwords[toks[i]]; s1 {
				continue
			}
			if _, s2 := p.stopwords[toks[i+1]]; s2 {
				continue
			}
			emit(toks[i]+"_"+toks[i+1], 1)
		}
	}
	if p.hashNgrams >= 3 {
		for i := 0; i+2 < len(toks); i++ {
			if _, s1 := p.stopwords[toks[i]]; s1 {
				continue
			}
			if _, s2 := p.stopwords[toks[i+1]]; s2 {
				continue
			}
			if _, s3 := p.stopwords[toks[i+2]]; s3 {
				continue
			}
			emit(toks[i]+"_"+toks[i+1]+"_"+toks[i+2], 1)
		}
	}

	normalize(vec)
	return vec
}

func (p *processor) tokenize(s string) []string {
	if p.lowercase {
		s = strings.ToLower(s)
	}
	parts := p.tokenSplit.Split(s, -1)
	out := make([]string, 0, len(parts))
	for _, t := range parts {
		if t == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// ---------------------- Utility / math ----------------------

func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum <= 0 {
		return
	}
	inv := float32(1.0 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

func f6(x float64) string {
	return strconv(x) //  reuse compact integer/float formatter
}

func strconv(x interface{}) string {
	switch t := x.(type) {
	case int64:
		return strconvI64(t)
	case int:
		return strconvI64(int64(t))
	case float64:
		return strconvF64(t)
	default:
		return ""
	}
}

func strconvI64(v int64) string { return strconvFormatInt(v, 10) }
func strconvF64(v float64) string {
	// compact float
	return strconvFormatFloat(v, 'f', 6, 64)
}

// Minimal local wrappers to keep the import section tidy:
func strconvFormatInt(i int64, base int) string { return strconvFmtInt(i, base) }
func strconvFormatFloat(f float64, fmt byte, prec, bitSize int) string {
	return strconvFmtFloat(f, fmt, prec, bitSize)
}

func strconvFmtInt(i int64, base int) string {
	return strconv.Itoa(int(i)) // acceptable for our ranges; if you prefer exact, import strconv.FormatInt
}

func strconvFmtFloat(f float64, fmt byte, prec, bitSize int) string {
	// For simplicity, import strconv here for accurate formatting:
	return strconv.FormatFloat(f, fmt, prec, bitSize)
}

// --- bring in strconv cleanly ---
import (
	"strconv"
)

// ---------------------- PCA loading ----------------------

type pcaFile struct {
	Rows int       `json:"rows"`
	Cols int       `json:"cols"`
	Data []float64 `json:"data"`
	Mean []float64 `json:"center,omitempty"`
	Std  []float64 `json:"scale,omitempty"`
}

func loadPCA(metricsNode any) (*pcaModel, error) {
	mp, ok := metricsNode.(map[string]any)
	if !ok {
		return nil, errors.New("bad metrics config")
	}
	pcanode, _ := mp["pca"].(map[string]any)
	if pcanode == nil {
		return nil, errors.New("no pca section")
	}

	// matrix_inline takes precedence
	if s, ok := pcanode["matrix_inline"].(string); ok && s != "" {
		var pf pcaFile
		if err := json.Unmarshal([]byte(s), &pf); err != nil {
			return nil, err
		}
		return pcaFromFile(pf)
	}
	// matrix_path
	if path, ok := pcanode["matrix_path"].(string); ok && path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var pf pcaFile
		if err := json.Unmarshal(b, &pf); err != nil {
			return nil, err
		}
		return pcaFromFile(pf)
	}
	return nil, errors.New("pca requires matrix_inline or matrix_path")
}

func pcaFromFile(pf pcaFile) (*pcaModel, error) {
	if pf.Rows <= 0 || pf.Cols <= 1 || len(pf.Data) != pf.Rows*pf.Cols {
		return nil, errors.New("invalid PCA matrix")
	}
	return &pcaModel{
		rows: pf.Rows,
		cols: pf.Cols,
		mat:  pf.Data,
		mean: pf.Mean,
		std:  pf.Std,
	}, nil
}

// ---------------------- nested config helpers ----------------------

func nestedStringSlice(extra map[string]any, k1, k2 string) ([]string, bool) {
	n1, ok := extra[k1].(map[string]any); if !ok { return nil, false }
	raw, ok := n1[k2].([]any); if !ok { return nil, false }
	out := make([]string, 0, len(raw))
	for _, it := range raw {
		if s, ok := it.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out, true
}
func nestedBool(extra map[string]any, k1, k2 string) (bool, bool) {
	n1, ok := extra[k1].(map[string]any); if !ok { return false, false }
	b, ok := n1[k2].(bool); return b, ok
}
func nestedBoolDefault(extra map[string]any, def bool, k1, k2, k3 string) bool {
	n1, ok := extra[k1].(map[string]any); if !ok { return def }
	n2, ok := n1[k2].(map[string]any); if !ok { return def }
	b, ok := n2[k3].(bool); if !ok { return def }
	return b
}
func nestedNumber(extra map[string]any, k1, k2 string) (float64, bool) {
	n1, ok := extra[k1].(map[string]any); if !ok { return 0, false }
	switch t := n1[k2].(type) {
	case float64: return t, true
	case int: return float64(t), true
	case int64: return float64(t), true
	case string:
		if f, err := strconv.ParseFloat(t, 64); err == nil { return f, true }
	}
	return 0, false
}
func nestedInt(extra map[string]any, k1, k2 string) (int, bool) {
	n1, ok := extra[k1].(map[string]any); if !ok { return 0, false }
	switch t := n1[k2].(type) {
	case int: return t, true
	case int64: return int(t), true
	case float64: return int(t), true
	case string:
		if i, err := strconv.Atoi(t); err == nil { return i, true }
	}
	return 0, false
}

// ---------------------- Fallback summary ----------------------

func (p *processor) defaultSummary(a model.Aggregate) string {
	sb := strings.Builder{}
	sb.WriteString("service ")
	sb.WriteString(a.Service)
	sb.WriteString(" window ")
	sb.WriteString(time.Unix(a.WindowStart, 0).UTC().Format(time.RFC3339))
	sb.WriteString("-")
	sb.WriteString(time.Unix(a.WindowEnd, 0).UTC().Format(time.RFC3339))
	sb.WriteString(" p50="); sb.WriteString(f6(a.P50))
	sb.WriteString(" p95="); sb.WriteString(f6(a.P95))
	sb.WriteString(" p99="); sb.WriteString(f6(a.P99))
	sb.WriteString(" rps="); sb.WriteString(f6(a.RPS))
	sb.WriteString(" err_rate="); sb.WriteString(f6(a.ErrorRate))
	return sb.String()
}