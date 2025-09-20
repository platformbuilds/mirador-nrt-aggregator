package pipeline

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

type stubReceiver struct {
	envelopes []model.Envelope
	started   atomic.Bool
}

func (sr *stubReceiver) Start(ctx context.Context, out chan<- model.Envelope) error {
	sr.started.Store(true)
	for _, e := range sr.envelopes {
		select {
		case <-ctx.Done():
			return nil
		case out <- e:
		}
	}
	<-ctx.Done()
	return nil
}

type stubProcessor struct {
	transform func(any) (any, bool)
}

func (sp *stubProcessor) Start(ctx context.Context, in <-chan any, out chan<- any) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case v, ok := <-in:
			if !ok {
				return nil
			}
			if sp.transform != nil {
				if nv, ok := sp.transform(v); ok {
					select {
					case <-ctx.Done():
						return nil
					case out <- nv:
					}
				}
			}
		}
	}
}

type stubExporter struct {
	mu        sync.Mutex
	received  []model.Aggregate
	gotFirst  chan struct{}
	startErr  error
	startOnce sync.Once
}

func newStubExporter() *stubExporter {
	return &stubExporter{gotFirst: make(chan struct{}, 1)}
}

func (se *stubExporter) Start(ctx context.Context, in <-chan model.Aggregate) error {
	if se.startErr != nil {
		return se.startErr
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case agg, ok := <-in:
			if !ok {
				return nil
			}
			se.mu.Lock()
			se.received = append(se.received, agg)
			se.mu.Unlock()
			se.startOnce.Do(func() {
				se.gotFirst <- struct{}{}
			})
		}
	}
}

func (se *stubExporter) all() []model.Aggregate {
	se.mu.Lock()
	defer se.mu.Unlock()
	cp := make([]model.Aggregate, len(se.received))
	copy(cp, se.received)
	return cp
}

func TestBuildAndRunReturnsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &config.Config{
		Receivers:  map[string]config.ReceiverCfg{},
		Processors: map[string]config.ProcessorCfg{},
		Exporters:  map[string]config.ExporterCfg{},
		Pipelines:  map[string]config.PipelineCfg{},
	}

	if err := BuildAndRun(ctx, cfg); err != nil {
		t.Fatalf("BuildAndRun returned error: %v", err)
	}
}

func TestRunSinglePipelineHappyPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rx := &stubReceiver{envelopes: []model.Envelope{{Kind: model.KindMetrics, Bytes: []byte("payload")}}}
	proc := &stubProcessor{transform: func(v any) (any, bool) {
		env, ok := v.(model.Envelope)
		if !ok {
			return nil, false
		}
		return model.Aggregate{Service: string(env.Bytes), SummaryText: "ok"}, true
	}}
	exp := newStubExporter()

	cfg := config.PipelineCfg{
		Receivers:  []string{"rx"},
		Processors: []string{"proc"},
		Exporters:  []string{"exp"},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := runSinglePipeline(ctx, "test", cfg, map[string]Receiver{"rx": rx}, map[string]Processor{"proc": proc}, map[string]Exporter{"exp": exp}); err != nil {
			t.Errorf("runSinglePipeline error: %v", err)
		}
	}()

	select {
	case <-exp.gotFirst:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for exporter to receive aggregate")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not stop after context cancel")
	}

	aggs := exp.all()
	if len(aggs) != 1 {
		t.Fatalf("expected 1 aggregate, got %d", len(aggs))
	}
	if aggs[0].Service != "payload" {
		t.Fatalf("unexpected aggregate: %+v", aggs[0])
	}
}

func TestRunSinglePipelineMissingComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.PipelineCfg{Receivers: []string{"missing"}}

	err := runSinglePipeline(ctx, "test", cfg, map[string]Receiver{}, map[string]Processor{}, map[string]Exporter{})
	if err == nil {
		t.Fatalf("expected error for missing receiver")
	}
	if !strings.Contains(err.Error(), "receiver \"missing\" not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildReceiversUnknownType(t *testing.T) {
	cfg := &config.Config{Receivers: map[string]config.ReceiverCfg{
		"bad": {Type: "does-not-exist"},
	}}
	if _, err := buildReceivers(cfg); err == nil {
		t.Fatal("expected error for unknown receiver")
	}
}

func TestBuildProcessorsUnknownType(t *testing.T) {
	cfg := &config.Config{Processors: map[string]config.ProcessorCfg{
		"bad": {Type: "nope"},
	}}
	if _, err := buildProcessors(cfg); err == nil {
		t.Fatal("expected error for unknown processor")
	}
}

func TestBuildExportersUnknownType(t *testing.T) {
	cfg := &config.Config{Exporters: map[string]config.ExporterCfg{
		"bad": {Type: "nope"},
	}}
	if _, err := buildExporters(cfg); err == nil {
		t.Fatal("expected error for unknown exporter")
	}
}

func TestEnvelopeToAny(t *testing.T) {
	in := make(chan model.Envelope, 1)
	in <- model.Envelope{Kind: model.KindMetrics}
	close(in)

	ch := envelopeToAny(in)
	v, ok := <-ch
	if !ok {
		t.Fatal("expected value from envelopeToAny")
	}
	if _, ok := v.(model.Envelope); !ok {
		t.Fatalf("expected model.Envelope, got %T", v)
	}
}
