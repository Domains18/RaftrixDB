package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// NodeConfig holds identity and address for a single node.
type NodeConfig struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

// RaftConfig holds tuning parameters for the Raft algorithm.
type RaftConfig struct {
	// ElectionTimeoutMinMs is the minimum election timeout in milliseconds.
	// Must be well above the network round-trip time.
	ElectionTimeoutMinMs int `yaml:"election_timeout_min_ms"`
	// ElectionTimeoutMaxMs is the maximum election timeout in milliseconds.
	ElectionTimeoutMaxMs int `yaml:"election_timeout_max_ms"`
	// HeartbeatIntervalMs is how often the leader sends heartbeats.
	// Must be much less than ElectionTimeoutMinMs.
	HeartbeatIntervalMs int `yaml:"heartbeat_interval_ms"`
	// SnapshotThreshold triggers a snapshot after this many log entries.
	SnapshotThreshold int `yaml:"snapshot_threshold"`
}

// StorageConfig configures the storage backend.
type StorageConfig struct {
	// Type is "memory" (testing) or "persistent" (BoltDB).
	Type    string `yaml:"type"`
	DataDir string `yaml:"data_dir"`
}

// ServerConfig configures the HTTP API server exposed to clients.
type ServerConfig struct {
	Addr string `yaml:"addr"`
}

// Config is the top-level configuration for a RaftrixDB node.
type Config struct {
	Node    NodeConfig    `yaml:"node"`
	Peers   []NodeConfig  `yaml:"peers"`
	Raft    RaftConfig    `yaml:"raft"`
	Storage StorageConfig `yaml:"storage"`
	Server  ServerConfig  `yaml:"server"`
}

// Load reads and parses a YAML config file, applying defaults for
// any missing values.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	applyDefaults(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// applyDefaults fills in sensible defaults from the Raft paper.
func applyDefaults(cfg *Config) {
	if cfg.Raft.ElectionTimeoutMinMs == 0 {
		cfg.Raft.ElectionTimeoutMinMs = 150
	}
	if cfg.Raft.ElectionTimeoutMaxMs == 0 {
		cfg.Raft.ElectionTimeoutMaxMs = 300
	}
	if cfg.Raft.HeartbeatIntervalMs == 0 {
		cfg.Raft.HeartbeatIntervalMs = 50
	}
	if cfg.Raft.SnapshotThreshold == 0 {
		cfg.Raft.SnapshotThreshold = 10_000
	}
	if cfg.Storage.Type == "" {
		cfg.Storage.Type = "persistent"
	}
	if cfg.Storage.DataDir == "" {
		cfg.Storage.DataDir = "./data"
	}
	if cfg.Server.Addr == "" {
		cfg.Server.Addr = ":8080"
	}
}

func validate(cfg *Config) error {
	if cfg.Node.ID == "" {
		return fmt.Errorf("node.id is required")
	}
	if cfg.Node.Addr == "" {
		return fmt.Errorf("node.addr is required")
	}
	if cfg.Raft.ElectionTimeoutMinMs >= cfg.Raft.ElectionTimeoutMaxMs {
		return fmt.Errorf("election_timeout_min_ms must be < election_timeout_max_ms")
	}
	if cfg.Raft.HeartbeatIntervalMs >= cfg.Raft.ElectionTimeoutMinMs {
		return fmt.Errorf("heartbeat_interval_ms must be < election_timeout_min_ms")
	}
	return nil
}