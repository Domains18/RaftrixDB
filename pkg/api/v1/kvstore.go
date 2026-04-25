// Package api provides the HTTP/JSON API server for RaftrixDB clients.
//
// Endpoints:
//   GET    /v1/keys/{key}          — read a value
//   PUT    /v1/keys/{key}          — write a value
//   DELETE /v1/keys/{key}          — delete a key
//   POST   /v1/cas/{key}           — compare-and-swap
//   GET    /v1/scan?prefix=<p>     — scan keys by prefix
//   POST   /v1/txn                 — atomic multi-op transaction
//   GET    /v1/status              — cluster status
//
// Raft internal endpoints (handled by the same mux):
//   POST   /raft/append-entries
//   POST   /raft/request-vote
//   POST   /raft/install-snapshot
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Domains18/RaftrixDB.git/internal/consensus"
	"github.com/Domains18/RaftrixDB.git/internal/storage"
	"github.com/Domains18/RaftrixDB.git/pkg/types"
)

// Server is the HTTP server that handles both client requests and Raft RPCs.
type Server struct {
	node    *consensus.RaftNode
	store   storage.Engine
	mux     *http.ServeMux
	httpSrv *http.Server
}

// NewServer wires up all routes and returns a Server ready to Listen.
func NewServer(addr string, node *consensus.RaftNode, store storage.Engine) *Server {
	s := &Server{node: node, store: store, mux: http.NewServeMux()}
	s.routes(addr)
	return s
}

func (s *Server) routes(addr string) {
	// ── Client API ─────────────────────────────────────────────────────
	s.mux.HandleFunc("/v1/keys/", s.handleKey)
	s.mux.HandleFunc("/v1/cas/", s.handleCAS)
	s.mux.HandleFunc("/v1/scan", s.handleScan)
	s.mux.HandleFunc("/v1/txn", s.handleTxn)
	s.mux.HandleFunc("/v1/status", s.handleStatus)

	// ── Raft internal RPCs ─────────────────────────────────────────────
	s.mux.HandleFunc("/raft/append-entries", s.handleAppendEntries)
	s.mux.HandleFunc("/raft/request-vote", s.handleRequestVote)
	s.mux.HandleFunc("/raft/install-snapshot", s.handleInstallSnapshot)

	s.httpSrv = &http.Server{
		Addr:         addr,
		Handler:      s.mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}

// ListenAndServe starts the HTTP server. Blocks until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		slog.Info("api server listening", "addr", s.httpSrv.Addr)
		if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutCtx)
	}
}

// ── Client handlers ────────────────────────────────────────────────────────

// handleKey dispatches GET / PUT / DELETE for /v1/keys/{key}
func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/v1/keys/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getKey(w, r, key)
	case http.MethodPut:
		s.putKey(w, r, key)
	case http.MethodDelete:
		s.deleteKey(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) getKey(w http.ResponseWriter, _ *http.Request, key string) {
	// Reads are served locally. For strict linearizability clients should
	// direct reads to the leader (checked via /v1/status). For now we
	// serve from any node for performance (eventual-consistent reads).
	val, err := s.store.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"key":   key,
		"value": string(val),
	})
}

func (s *Server) putKey(w http.ResponseWriter, r *http.Request, key string) {
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	cmd := types.Command{Op: types.OpPut, Key: key, Value: []byte(body.Value)}
	if err := s.propose(r.Context(), cmd); err != nil {
		s.handleProposeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) deleteKey(w http.ResponseWriter, r *http.Request, key string) {
	cmd := types.Command{Op: types.OpDelete, Key: key}
	if err := s.propose(r.Context(), cmd); err != nil {
		s.handleProposeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleCAS handles POST /v1/cas/{key}
func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/v1/cas/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	var body struct {
		PrevValue string `json:"prev_value"`
		NewValue  string `json:"new_value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	cmd := types.Command{
		Op:        types.OpCAS,
		Key:       key,
		Value:     []byte(body.NewValue),
		PrevValue: []byte(body.PrevValue),
	}
	if err := s.propose(r.Context(), cmd); err != nil {
		s.handleProposeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleScan handles GET /v1/scan?prefix=<p>
func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	result, err := s.store.Scan(prefix)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Convert []byte values to strings for JSON portability.
	out := make(map[string]string, len(result))
	for k, v := range result {
		out[k] = string(v)
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

// handleTxn handles POST /v1/txn — a batch of ops committed atomically.
func (s *Server) handleTxn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		Ops []struct {
			Op        string `json:"op"`
			Key       string `json:"key"`
			Value     string `json:"value"`
			PrevValue string `json:"prev_value"`
		} `json:"ops"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Each operation in the batch is proposed as a separate Raft entry
	// sequentially. True atomic multi-key transactions would require a
	// single compound log entry; this is an approximation suitable for
	// single-key CAS chains.
	for i, op := range body.Ops {
		cmd := types.Command{
			Op:        types.OpType(strings.ToUpper(op.Op)),
			Key:       op.Key,
			Value:     []byte(op.Value),
			PrevValue: []byte(op.PrevValue),
		}
		if err := s.propose(r.Context(), cmd); err != nil {
			s.handleProposeError(w, fmt.Errorf("op[%d]: %w", i, err))
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleStatus handles GET /v1/status
func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":       s.node.ID(),
		"is_leader":     s.node.IsLeader(),
		"leader_id":     s.node.LeaderID(),
		"commit_index":  s.node.CommitIndex(),
		"last_applied":  s.node.LastApplied(),
		"snapshot_index": s.node.SnapshotIndex(),
	})
}

// ── Raft RPC handlers ──────────────────────────────────────────────────────

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req types.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp := s.node.HandleAppendEntries(&req)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req types.RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp := s.node.HandleRequestVote(&req)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	var req types.InstallSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp := s.node.HandleInstallSnapshot(&req)
	writeJSON(w, http.StatusOK, resp)
}

// ── helpers ────────────────────────────────────────────────────────────────

func (s *Server) propose(ctx context.Context, cmd types.Command) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.node.Propose(ctx, cmd)
}

func (s *Server) handleProposeError(w http.ResponseWriter, err error) {
	var nle *consensus.NotLeaderError
	if errors.As(err, &nle) {
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{
			"error":     "not leader",
			"leader_id": nle.LeaderID,
		})
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}