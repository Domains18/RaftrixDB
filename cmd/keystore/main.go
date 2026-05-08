// RaftrixDB — main entrypoint.
//
// Usage:
//
//	raftrixdb -config ./config/default.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Domains18/RaftrixDB.git/config"
	"github.com/Domains18/RaftrixDB.git/internal/cluster"
	"github.com/Domains18/RaftrixDB.git/internal/consensus"
	"github.com/Domains18/RaftrixDB.git/internal/storage"
	api "github.com/Domains18/RaftrixDB.git/pkg/api/v1"
	"github.com/Domains18/RaftrixDB.git/pkg/types"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "./config/default.yaml", "path to config YAML file")
	flag.Parse()

	// ── Load configuration ────────────────────────────────────────────────
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	slog.Info("starting RaftrixDB",
		"node_id", cfg.Node.ID,
		"raft_addr", cfg.Node.Addr,
		"api_addr", cfg.Server.Addr,
		"peers", len(cfg.Peers),
	)

	// ── Storage engine ────────────────────────────────────────────────────
	var store storage.Engine
	switch cfg.Storage.Type {
	case "memory":
		store = storage.NewMemoryEngine()
		slog.Info("using in-memory storage (non-persistent)")
	case "persistent":
		kvPath := fmt.Sprintf("%s/kv.db", cfg.Storage.DataDir)
		boltStore, err := storage.NewBoltEngine(kvPath)
		if err != nil {
			return fmt.Errorf("opening kv storage: %w", err)
		}
		defer boltStore.Close()
		store = boltStore
		slog.Info("using persistent storage (BoltDB)", "path", kvPath)
	default:
		return fmt.Errorf("unknown storage type: %q (want memory or persistent)", cfg.Storage.Type)
	}

	// ── Cluster transport ─────────────────────────────────────────────────
	peers := make([]types.Peer, len(cfg.Peers))
	for i, p := range cfg.Peers {
		peers[i] = types.Peer{ID: types.NodeID(p.ID), Addr: p.Addr}
	}
	transport := cluster.NewHTTPTransport(peers)

	// Start the heartbeat health tracker.
	tracker := cluster.NewHeartbeatTracker(transport.Members())
	tracker.Start()
	defer tracker.Stop()

	// ── Raft node ─────────────────────────────────────────────────────────
	raftNode, err := consensus.NewRaftNode(cfg, store, transport)
	if err != nil {
		return fmt.Errorf("creating raft node: %w", err)
	}
	raftNode.Start()
	defer raftNode.Stop()

	// ── HTTP server (client API + Raft RPCs) ──────────────────────────────
	srv := api.NewServer(cfg.Server.Addr, raftNode, store)

	// Graceful shutdown on SIGINT / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.ListenAndServe(ctx); err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	slog.Info("shutdown complete", "node", cfg.Node.ID)
	return nil
}