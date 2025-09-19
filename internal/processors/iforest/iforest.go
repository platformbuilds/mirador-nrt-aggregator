package iforest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/yourorg/mirador-nrt-aggregator/internal/config"
	"github.com/yourorg/mirador-nrt-aggregator/internal/model"
)

// ---------------------------- Public processor ----------------------------

type processor struct {
	feats      []string
	threshold  float64
	normMode   string // "zscore" | "none"
	stats      *zstats
	forest     *Forest
	subsampleN float64

	mu sync.RWMutex
}

func New(cfg config.ProcessorCfg) *processor {
	feats := cfg.Features
	if len(feats) == 0 {
		feats = []string{"p99", "error_rate", "rps"}
	}
	normMode := "none"
	if v := cfg.ExtraString("normalization", "none"); v != "" {
		normMode = v
	}
	baseline := 3600
	if v, ok := cfg.Extra["baseline_window"]; ok {
		switch t := v.(type) {
		case int:
			baseline = t
		case int64:
			baseline = int(t)
		case float64:
			baseline = int(t)
		case string:
			if n, err := strconv.Atoi(t); err == nil {
				baseline = n
			}
		}
	}
	forest, _ := loadForest(cfg)
	subsample := 256.0
	if v, ok := cfg.Extra["subsample_size"]; ok {
		switch t := v.(type) {
		case int:
			subsample = float64(t)
		case int64:
			subsample = float64(t)
		case float64:
			subsample = t
		case string:
			if n, err := strconv.Atoi(t); err == nil {
				subsample = float64(n)
			}
		}
	}

	return &processor{
		feats:      feats,
		threshold:  cfg.Threshold,
		normMode:   normMode,
		stats:      newZ(baseline),
		forest:     forest,
		subsampleN: subsample,
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
				// Pass through anything else untouched
				out <- v
				continue
			}
			// Build feature vector
			vec := p.featuresOf(a)

			// Optional normalization (rolling per service)
			if p.normMode == "zscore" {
				vec = p.stats.apply(a.Service, a.WindowEnd, vec)
			}

			// Score
			score := p.score(vec)
			a.AnomalyScore = score

			// Forward
			out <- a
		}
	}
}

// ---------------------------- Feature mapping ----------------------------

func (p *processor) featuresOf(a model.Aggregate) []float64 {
	x := make([]float64, 0, len(p.feats))
	for _, f := range p.feats {
		switch f {
		case "p50":
			x = append(x, a.P50)
		case "p95":
			x = append(x, a.P95)
		case "p99":
			x = append(x, a.P99)
		case "rps":
			x = append(x, a.RPS)
		case "error_rate":
			x = append(x, a.ErrorRate)
		case "count":
			x = append(x, float64(a.Count))
		default:
			// Unknown feature → append 0 to keep vector length stable
			x = append(x, 0)
		}
	}
	return x
}

// ---------------------------- Scoring logic ----------------------------

func (p *processor) score(x []float64) float64 {
	p.mu.RLock()
	f := p.forest
	n := p.subsampleN
	p.mu.RUnlock()

	if f == nil || len(f.Trees) == 0 {
		return 0.0
	}
	if n <= 1 {
		n = 256
	}

	var sumDepth float64
	for _, t := range f.Trees {
		sumDepth += pathLength(t, x)
	}
	avgDepth := sumDepth / float64(len(f.Trees))

	// Expected path length for unsuccessful search in a Binary Search Tree:
	// c(n) = 2 * (H(n-1)) - (2*(n-1)/n), with H(m) ≈ ln(m) + gamma
	// Use this to convert path length to anomaly score in [0,1].
	cn := cOfN(n)
	score := math.Exp(-avgDepth / cn)
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}

func cOfN(n float64) float64 {
	if n <= 1 {
		return 1
	}
	// Harmonic number approximation: H(n-1) ≈ ln(n-1) + gamma
	gamma := 0.57721566490153286060
	return 2.0*(math.Log(n-1.0)+gamma) - (2.0*(n-1.0))/n
}

// ---------------------------- Forest model ----------------------------

// Forest is a compact axis-aligned Isolation Forest representation.
// JSON shape example:
//
//	{
//	  "version": "iforest-v1",
//	  "trees": [
//	    { "nodes": [ {"f":0,"t":0.15,"l":1,"r":2}, {"leaf":true,"size":128}, {"leaf":true,"size":12} ] },
//	    ...
//	  ]
//	}
type Forest struct {
	Version string `json:"version"`
	Trees   []Tree `json:"trees"`
}

type Tree struct {
	Nodes []Node `json:"nodes"`
}

type Node struct {
	F    int     `json:"f,omitempty"` // feature index
	T    float64 `json:"t,omitempty"` // threshold
	L    int     `json:"l,omitempty"` // left child
	R    int     `json:"r,omitempty"` // right child
	Leaf bool    `json:"leaf,omitempty"`
	Size int     `json:"size,omitempty"` // training subset size at leaf (optional)
}

func pathLength(t Tree, x []float64) float64 {
	i := 0
	depth := 0.0
	maxDepth := 64.0
	for i >= 0 && i < len(t.Nodes) && depth < maxDepth {
		n := t.Nodes[i]
		if n.Leaf || (n.L == 0 && n.R == 0 && n.F == 0 && n.T == 0) {
			return depth
		}
		// Defensive feature indexing
		val := 0.0
		if n.F >= 0 && n.F < len(x) {
			val = x[n.F]
		}
		if val <= n.T {
			i = n.L
		} else {
			i = n.R
		}
		depth += 1.0
	}
	return depth
}

// ---------------------------- Model loading ----------------------------

func loadForest(cfg config.ProcessorCfg) (*Forest, error) {
	// 1) explicit extra.model_path
	if pth := cfg.ExtraString("model_path", ""); pth != "" {
		return readForestFile(pth)
	}
	// 2) extra.model: { path: "..." } or inline JSON
	if raw, ok := cfg.Extra["model"]; ok {
		switch v := raw.(type) {
		case string:
			// Could be inline JSON string
			var f Forest
			if err := json.Unmarshal([]byte(v), &f); err == nil && len(f.Trees) > 0 {
				return &f, nil
			}
			// Or a path
			return readForestFile(v)
		case map[string]any:
			if pth, ok := v["path"].(string); ok && pth != "" {
				return readForestFile(pth)
			}
			// Inline structure
			b, _ := json.Marshal(v)
			var f Forest
			if err := json.Unmarshal(b, &f); err == nil && len(f.Trees) > 0 {
				return &f, nil
			}
		}
	}
	// 3) extra.model_inline: raw JSON
	if s, ok := cfg.Extra["model_inline"].(string); ok && s != "" {
		var f Forest
		if err := json.Unmarshal([]byte(s), &f); err == nil && len(f.Trees) > 0 {
			return &f, nil
		}
	}
	// No model provided → operate in "no-op" mode (score=0)
	return nil, errors.New("iforest: no model provided; running with neutral scores")
}

func readForestFile(path string) (*Forest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read model: %w", err)
	}
	var f Forest
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("parse model json: %w", err)
	}
	if len(f.Trees) == 0 {
		return nil, errors.New("model has zero trees")
	}
	return &f, nil
}

// ---------------------------- Rolling z-score ----------------------------

type zstats struct {
	winSec int
	mu     sync.Mutex
	// per-service rolling stats of each feature index
	// svc -> idx -> welford
	data map[string][]*welford
	// housekeeping
	lastTrim int64
}

func newZ(win int) *zstats {
	if win <= 0 {
		win = 3600
	}
	return &zstats{
		winSec: win,
		data:   map[string][]*welford{},
	}
}

// apply returns z-scored features (per service) using an online Welford estimator.
// This simple implementation grows without bounds; in practice you might want TTL per service.
// For now we keep it lean and effective for NRT usage.
func (z *zstats) apply(svc string, ts int64, vec []float64) []float64 {
	z.mu.Lock()
	defer z.mu.Unlock()

	ws, ok := z.data[svc]
	if !ok || len(ws) != len(vec) {
		ws = make([]*welford, len(vec))
		for i := range ws {
			ws[i] = &welford{}
		}
		z.data[svc] = ws
	}

	out := make([]float64, len(vec))
	for i, v := range vec {
		ws[i].add(v)
		m, sd := ws[i].mean, ws[i].stddev()
		if sd > 1e-9 {
			out[i] = (v - m) / sd
		} else {
			out[i] = 0
		}
	}

	// periodic light cleanup opportunity (no-op placeholder)
	if ts-z.lastTrim > int64(z.winSec) {
		z.lastTrim = ts
		// (future: implement TTL/decay if needed)
	}
	return out
}

type welford struct {
	n    float64
	mean float64
	m2   float64
}

func (w *welford) add(x float64) {
	w.n++
	d := x - w.mean
	w.mean += d / w.n
	w.m2 += d * (x - w.mean)
}

func (w *welford) variance() float64 {
	if w.n < 2 {
		return 0
	}
	return w.m2 / (w.n - 1)
}

func (w *welford) stddev() float64 {
	return math.Sqrt(w.variance())
}

// ---------------------------- Math helpers ----------------------------

var (
	_ = time.Now // keep time import for possible future decay/TTL
)
