package logsum

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

// processor aggregates JSON logs into per-service, fixed-size windows.
type processor struct {
	winSec    int
	svcKey    string
	lvlKey    string
	errLevels map[string]struct{}
	userKeys  []string

	// percentile field (optional) to compute latency-like quantiles
	quantField string
	// If quantField is numeric (int/float), we can optionally compute simple quantiles using a compact reservoir.
	// For simplicity and low overhead here, we provide a tiny fixed-cap reservoir per service.
	reservoirCap int

	// top-k categorical keys to summarize
	topKeys  []string
	topLimit int

	// state: per service window
	state map[string]*wState
}

type wState struct {
	start int64
	end   int64

	total uint64
	errs  uint64

	seenUsers map[string]struct{}

	top map[string]map[string]uint64 // key -> value -> count

	// compact reservoir for quantiles (optional)
	res []float64
}

func New(cfg config.ProcessorCfg) *processor {
	w := cfg.WindowSeconds
	if w <= 0 {
		w = 60
	}
	// configurable fields
	svcKey := pick(cfg, "service_field", "service")
	lvlKey := pick(cfg, "level_field", "level")
	quantField := cfg.ExtraString("quantile_field", "") // e.g. "latency_ms"
	topLimit := 5
	if v, ok := cfg.Extra["topk_limit"]; ok {
		switch t := v.(type) {
		case int:
			topLimit = t
		case int64:
			topLimit = int(t)
		case float64:
			topLimit = int(t)
		case string:
			if n, err := strconv.Atoi(t); err == nil {
				topLimit = n
			}
		}
	}
	var topKeys []string
	if arr, ok := cfg.Extra["topk_fields"].([]any); ok {
		for _, it := range arr {
			if s, ok := it.(string); ok && s != "" {
				topKeys = append(topKeys, s)
			}
		}
	}

	// error levels set (default: error, fatal)
	errLevels := map[string]struct{}{"error": {}, "fatal": {}}
	if arr, ok := cfg.Extra["error_levels"].([]any); ok {
		errLevels = map[string]struct{}{}
		for _, it := range arr {
			if s, ok := it.(string); ok && s != "" {
				errLevels[strings.ToLower(s)] = struct{}{}
			}
		}
	}

	// user identity fields for unique user counting
	userKeys := []string{"user_id", "uid", "user", "customer_id", "account_id"}
	if arr, ok := cfg.Extra["user_id_fields"].([]any); ok {
		userKeys = userKeys[:0]
		for _, it := range arr {
			if s, ok := it.(string); ok && s != "" {
				userKeys = append(userKeys, s)
			}
		}
	}

	reservoirCap := 256
	if v, ok := cfg.Extra["reservoir_cap"].(int); ok && v > 0 {
		reservoirCap = v
	}

	return &processor{
		winSec:       w,
		svcKey:       svcKey,
		lvlKey:       lvlKey,
		errLevels:    errLevels,
		userKeys:     userKeys,
		quantField:   quantField,
		reservoirCap: reservoirCap,
		topKeys:      topKeys,
		topLimit:     topLimit,
		state:        map[string]*wState{},
	}
}

func (p *processor) Start(ctx context.Context, in <-chan any, out chan<- any) error {
	defer close(out)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	winStart := trunc(time.Now().Unix(), int64(p.winSec))

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
				// Pass through anything not a JSON log envelope
				out <- v
				continue
			}
			p.consume(env.Bytes, winStart)

		case now := <-ticker.C:
			if now.Unix() >= winStart+int64(p.winSec) {
				p.flush(out, winStart)
				winStart = trunc(now.Unix(), int64(p.winSec))
			}
		}
	}
}

func (p *processor) consume(raw []byte, winStart int64) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return
	}

	// service identity
	svc := getStr(obj, p.svcKey, "service.name", "svc", "app", "application")
	if svc == "" {
		svc = "unknown"
	}
	st := p.ensure(svc, winStart)

	// level -> errors
	lvl := strings.ToLower(getStr(obj, p.lvlKey))
	if _, isErr := p.errLevels[lvl]; isErr {
		st.errs++
	}
	st.total++

	// unique users
	if uid := firstNonEmpty(obj, p.userKeys...); uid != "" {
		if st.seenUsers == nil {
			st.seenUsers = map[string]struct{}{}
		}
		st.seenUsers[uid] = struct{}{}
	}

	// top-k categorical fields
	if st.top == nil {
		st.top = map[string]map[string]uint64{}
	}
	for _, k := range p.topKeys {
		val := getStr(obj, k)
		if val == "" {
			continue
		}
		m := st.top[k]
		if m == nil {
			m = map[string]uint64{}
			st.top[k] = m
		}
		m[val]++
	}

	// optional numeric quantile field (e.g. latency)
	if p.quantField != "" {
		if f, ok := getFloat(obj, p.quantField); ok {
			// simple reservoir sampling with cap; for NRT this is usually enough
			if len(st.res) < p.reservoirCap {
				st.res = append(st.res, f)
			} else {
				// replace a random element — cheap LCG using timestamp for determinism-free sampling
				// (not cryptographically strong; fine for sketching)
				i := int(time.Now().UnixNano() % int64(p.reservoirCap))
				st.res[i] = f
			}
		}
	}
}

func (p *processor) flush(out chan<- any, winStart int64) {
	winEnd := winStart + int64(p.winSec)

	for svc, st := range p.state {
		labels := map[string]string{}

		// top-k summaries
		for _, k := range p.topKeys {
			if m, ok := st.top[k]; ok && len(m) > 0 {
				type kv struct {
					val string
					n   uint64
				}
				buf := make([]kv, 0, len(m))
				for v, n := range m {
					buf = append(buf, kv{v, n})
				}
				sort.Slice(buf, func(i, j int) bool { return buf[i].n > buf[j].n })
				max := p.topLimit
				if max > len(buf) {
					max = len(buf)
				}
				parts := make([]string, 0, max)
				for i := 0; i < max; i++ {
					parts = append(parts, buf[i].val+":"+strconv.FormatUint(buf[i].n, 10))
				}
				if len(parts) > 0 {
					labels["top_"+k] = strings.Join(parts, ",")
				}
			}
		}

		// unique user count
		if st.seenUsers != nil {
			labels["unique_users"] = strconv.Itoa(len(st.seenUsers))
		} else {
			labels["unique_users"] = "0"
		}

		// quantiles (if quantField provided)
		p50, p95, p99 := 0.0, 0.0, 0.0
		if len(st.res) > 0 {
			cp := make([]float64, len(st.res))
			copy(cp, st.res)
			sort.Float64s(cp)
			p50 = percentile(cp, 0.50)
			p95 = percentile(cp, 0.95)
			p99 = percentile(cp, 0.99)
		}

		// event rate and error rate
		rps := float64(st.total) / float64(p.winSec)
		errRate := 0.0
		if st.total > 0 {
			errRate = float64(st.errs) / float64(st.total)
		}

		agg := model.Aggregate{
			Service:     svc,
			WindowStart: winStart,
			WindowEnd:   winEnd,
			Labels:      labels,
			Locator:     "{}", // free-form for deep-links later
			Count:       st.total,
			RPS:         rps,
			ErrorRate:   errRate,
			P50:         p50,
			P95:         p95,
			P99:         p99,
			SummaryText: buildSummaryText(svc, st.total, errRate, labels),
		}
		out <- agg
	}

	// reset window state
	p.state = map[string]*wState{}
}

func (p *processor) ensure(svc string, winStart int64) *wState {
	if st, ok := p.state[svc]; ok {
		return st
	}
	st := &wState{
		start: winStart,
		end:   winStart,
		top:   map[string]map[string]uint64{},
		res:   make([]float64, 0, p.reservoirCap),
	}
	p.state[svc] = st
	return st
}

// -------------------- helpers --------------------

func buildSummaryText(svc string, total uint64, errRate float64, labels map[string]string) string {
	sb := strings.Builder{}
	sb.WriteString("logs summary: service=")
	sb.WriteString(svc)
	sb.WriteString(" total=")
	sb.WriteString(strconv.FormatUint(total, 10))
	sb.WriteString(" error_rate=")
	sb.WriteString(strconv.FormatFloat(errRate, 'f', 4, 64))
	// include first top-* if present
	for k, v := range labels {
		if strings.HasPrefix(k, "top_") {
			sb.WriteString(" ")
			sb.WriteString(k)
			sb.WriteString("=")
			sb.WriteString(v)
			break
		}
	}
	return sb.String()
}

func trunc(ts int64, win int64) int64 { return ts - (ts % win) }

func getStr(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case string:
				return t
			case float64:
				return strconv.FormatFloat(t, 'f', -1, 64)
			case json.Number:
				return t.String()
			}
		}
	}
	return ""
}

func getFloat(m map[string]any, key string) (float64, bool) {
	if v, ok := m[key]; ok {
		switch t := v.(type) {
		case float64:
			return t, true
		case int:
			return float64(t), true
		case int64:
			return float64(t), true
		case json.Number:
			f, err := t.Float64()
			if err == nil {
				return f, true
			}
		case string:
			if f, err := strconv.ParseFloat(t, 64); err == nil {
				return f, true
			}
		}
	}
	return 0, false
}

func firstNonEmpty(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := getStr(m, k); s != "" {
			return s
		}
	}
	return ""
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	pos := q * float64(len(sorted)-1)
	l := int(pos)
	r := l + 1
	if r >= len(sorted) {
		return sorted[l]
	}
	fr := pos - float64(l)
	return sorted[l]*(1-fr) + sorted[r]*fr
}

func pick(c config.ProcessorCfg, key, def string) string {
	if c.Extra != nil {
		if v, ok := c.Extra[key]; ok {
			if s, ok2 := v.(string); ok2 && s != "" {
				return s
			}
		}
	}
	return def
}
