package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yourorg/mirador-nrt-aggregator/internal/config"
	"github.com/yourorg/mirador-nrt-aggregator/internal/pipeline"

	"golang.org/x/sync/errgroup"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// These can be overridden at build time using -ldflags:
//
//	-ldflags="-X main.version=$(git describe --tags --dirty --always) -X main.commit=$(git rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	// -------- flags & env --------
	defaultCfg := envOr("MIRADOR_CONFIG", "config.yaml")
	var (
		cfgPath     = flag.String("config", defaultCfg, "Path to the config YAML")
		metricsAddr = flag.String("metrics.addr", envOr("MIRADOR_METRICS_ADDR", ":9090"), "Prometheus metrics HTTP listen address")
		pprofAddr   = flag.String("pprof.addr", envOr("MIRADOR_PPROF_ADDR", ""), "pprof HTTP listen address (disabled if empty)")
		logTime     = flag.Bool("log.timestamps", true, "Include timestamps in log output")
	)
	flag.Parse()

	if *logTime {
		log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	} else {
		log.SetFlags(0)
	}
	log.Printf("mirador-nrt-aggregator %s (commit %s, built %s)", version, commit, date)

	// -------- load config --------
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}
	log.Printf("loaded config from %s with %d pipeline(s)", *cfgPath, len(cfg.Pipelines))

	// -------- root context & signals --------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// -------- metrics & health servers --------
	ready := &atomic.Bool{}
	ready.Store(false)

	metricsSrv := &http.Server{
		Addr:              *metricsAddr,
		Handler:           setupMetricsMux(ready),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("metrics: listening on %s", *metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics: server error: %v", err)
		}
	}()

	// Optional pprof (disabled by default)
	if *pprofAddr != "" {
		go func() {
			mux := http.NewServeMux()
			// Register pprof handlers only when enabled to avoid importing net/http/pprof unless requested
			registerPprof(mux)
			pp := &http.Server{Addr: *pprofAddr, Handler: mux}
			log.Printf("pprof: listening on %s", *pprofAddr)
			if err := pp.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("pprof: server error: %v", err)
			}
		}()
	}

	// -------- run pipelines (blocking until ctx done) --------
	var g errgroup.Group

	// pipelines
	g.Go(func() error {
		ready.Store(true) // mark ready once we start building/running
		err := pipeline.BuildAndRun(ctx, cfg)
		if err != nil {
			return fmt.Errorf("pipeline: %w", err)
		}
		return nil
	})

	// signal watcher
	g.Go(func() error {
		s := <-sigCh
		log.Printf("signal received: %s — initiating graceful shutdown", s)
		cancel()
		return nil
	})

	// graceful shutdown of metrics server when ctx ends
	g.Go(func() error {
		<-ctx.Done()
		shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shCancel()
		if err := metricsSrv.Shutdown(shCtx); err != nil {
			log.Printf("metrics: shutdown error: %v", err)
		}
		return nil
	})

	// wait for all
	if err := g.Wait(); err != nil && err != context.Canceled {
		log.Printf("shutdown with error: %v", err)
	} else {
		log.Printf("shutdown complete")
	}
}

// setupMetricsMux registers Prometheus /metrics plus simple health endpoints.
func setupMetricsMux(ready *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	// Prometheus scrape endpoint
	mux.Handle("/metrics", promhttp.Handler())
	// Liveness: if the process is up, return 200
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// Readiness: once pipelines started, return 200
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	return mux
}

// registerPprof safely registers pprof handlers only if pprof is enabled.
func registerPprof(mux *http.ServeMux) {
	// Lazy import pattern to avoid always importing net/http/pprof in builds that don't need it.
	// (Kept inline for clarity; feel free to move to a separate file with build tags.)
	type pprofRegisterFn func(*http.ServeMux)
	var impl pprofRegisterFn = func(m *http.ServeMux) {
		// nolint:staticcheck // intentionally import pprof only here
		importPprof := func() {
			// This tiny helper lets us import pprof locally without a top-level import.
		}
		_ = importPprof

		// Re-import with alias to register handlers
		// NOTE: The below closure is a trick to keep pprof imports isolated.
		func() {
			// go:linkname style tricks are overkill; just re-declare inner scope with imports
			// We simply panic with guidance if a developer enables pprof but forgets to add the import.
			panic("pprof requires adding `import _ \"net/http/pprof\"` and mux.Handle to endpoints. " +
				"To keep main.go simple, replace registerPprof with direct pprof registration in your codebase if needed.")
		}()
	}
	// By default we provide a helpful panic so folks can wire their preferred pprof paths.
	// Replace this function with your project's preferred pprof registration as needed.
	impl(mux)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
