package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Root config object
type Config struct {
	Receivers  map[string]ReceiverCfg  `yaml:"receivers"`
	Processors map[string]ProcessorCfg `yaml:"processors"`
	Exporters  map[string]ExporterCfg  `yaml:"exporters"`
	Pipelines  map[string]PipelineCfg  `yaml:"pipelines"`
}

type ReceiverCfg struct {
	Name     string         `yaml:"-"`
	Type     string         `yaml:"type"`
	Endpoint string         `yaml:"endpoint,omitempty"`
	Brokers  []string       `yaml:"brokers,omitempty"`
	Topic    string         `yaml:"topic,omitempty"`
	Group    string         `yaml:"group,omitempty"`
	Extra    map[string]any `yaml:",inline"`
}

type ProcessorCfg struct {
	Name             string         `yaml:"-"`
	Type             string         `yaml:"type"`
	WindowSeconds    int            `yaml:"window_seconds,omitempty"`
	HistogramBuckets []float64      `yaml:"histogram_buckets,omitempty"`
	Dimensions       []string       `yaml:"dimensions,omitempty"`
	Quantiles        []float64      `yaml:"quantiles,omitempty"`
	Features         []string       `yaml:"features,omitempty"`
	NTrees           int            `yaml:"n_trees,omitempty"`
	Threshold        float64        `yaml:"threshold,omitempty"`
	Extra            map[string]any `yaml:",inline"`
}

type ExporterCfg struct {
	Name       string         `yaml:"-"`
	Type       string         `yaml:"type"`
	Endpoint   string         `yaml:"endpoint,omitempty"`
	Class      string         `yaml:"class,omitempty"`
	IDTemplate string         `yaml:"id_template,omitempty"`
	Extra      map[string]any `yaml:",inline"`
}

type PipelineCfg struct {
	Receivers  []string `yaml:"receivers"`
	Processors []string `yaml:"processors"`
	Exporters  []string `yaml:"exporters"`
}

// Load reads YAML config into a Config struct.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	// Normalize receivers
	for k, v := range cfg.Receivers {
		typ, name := splitKey(k)
		if v.Type == "" {
			v.Type = typ
		}
		if v.Name == "" {
			v.Name = name
		}
		if v.Extra == nil {
			v.Extra = map[string]any{}
		}
		cfg.Receivers[k] = v
	}

	// Normalize processors
	for k, v := range cfg.Processors {
		typ, name := splitKey(k)
		if v.Type == "" {
			v.Type = typ
		}
		if v.Name == "" {
			v.Name = name
		}
		if v.Extra == nil {
			v.Extra = map[string]any{}
		}
		cfg.Processors[k] = v
	}

	// Normalize exporters
	for k, v := range cfg.Exporters {
		typ, name := splitKey(k)
		if v.Type == "" {
			v.Type = typ
		}
		if v.Name == "" {
			v.Name = name
		}
		if v.Extra == nil {
			v.Extra = map[string]any{}
		}
		cfg.Exporters[k] = v
	}

	return &cfg, nil
}

// splitKey lets you write keys like "jsonlogs/http" in YAML.
// It splits into (type, name).
func splitKey(k string) (typ, name string) {
	if k == "" {
		return "", ""
	}
	parts := strings.SplitN(k, "/", 2)
	if len(parts) == 1 {
		return parts[0], parts[0]
	}
	return parts[0], parts[1]
}

// --- Helpers for reading typed extras ---

func (pc ProcessorCfg) ExtraString(key, def string) string {
	if pc.Extra == nil {
		return def
	}
	if v, ok := pc.Extra[key]; ok {
		if s, ok2 := v.(string); ok2 {
			return s
		}
	}
	return def
}

func (pc ProcessorCfg) ExtraBool(key string, def bool) bool {
	if pc.Extra == nil {
		return def
	}
	if v, ok := pc.Extra[key]; ok {
		if b, ok2 := v.(bool); ok2 {
			return b
		}
	}
	return def
}

// --- Utility ---

// ResolvePath returns an absolute path relative to the config file dir.
func ResolvePath(cfgPath, given string) string {
	if filepath.IsAbs(given) {
		return given
	}
	return filepath.Join(filepath.Dir(cfgPath), given)
}
