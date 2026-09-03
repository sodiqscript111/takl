package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Cluster struct {
	NodeID  string   `yaml:"node_id"`
	Bind    int      `yaml:"bind"`
	Join    []string `yaml:"join"`
	Peers   []string `yaml:"peers"`
	Profile string   `yaml:"profile"`
}

type Network struct {
	HTTPAddr      string `yaml:"http_addr"`
	SyncAddr      string `yaml:"sync_addr"`
	AdvertiseAddr string `yaml:"advertise_addr"`
}

type Storage struct {
	DBPath string `yaml:"db_path"`
}

type Execution struct {
	Backend  string `yaml:"backend"`
	Capacity int    `yaml:"capacity"`
	Region   string `yaml:"region"`
}

type Agent struct {
	Tick         time.Duration `yaml:"tick"`
	SyncInterval time.Duration `yaml:"sync_interval"`
}

type Config struct {
	Cluster   Cluster   `yaml:"cluster"`
	Network   Network   `yaml:"network"`
	Storage   Storage   `yaml:"storage"`
	Execution Execution `yaml:"execution"`
	Agent     Agent     `yaml:"agent"`
}

// Default returns a configuration with sensible default values.
func Default() *Config {
	return &Config{
		Cluster: Cluster{
			NodeID:  "runner-1",
			Bind:    7946,
			Join:    []string{},
			Peers:   []string{},
			Profile: "lan",
		},
		Network: Network{
			HTTPAddr:      "127.0.0.1:8090",
			SyncAddr:      "127.0.0.1:8100",
			AdvertiseAddr: "",
		},
		Storage: Storage{
			DBPath: "takl.db",
		},
		Execution: Execution{
			Backend:  "docker",
			Capacity: 4,
			Region:   "us-east-1",
		},
		Agent: Agent{
			Tick:         time.Second,
			SyncInterval: 2 * time.Second,
		},
	}
}

// Load reads a YAML configuration file from the given path.
// It merges the parsed configuration over the provided default config.
func Load(path string, base *Config) (*Config, error) {
	if path == "" {
		return base, nil
	}

	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// If file is missing, just return base config
			return base, nil
		}
		return nil, fmt.Errorf("read config file: %w", err)
	}

	// Unmarshal overrides base
	if err := yaml.Unmarshal(b, base); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	return base, nil
}
