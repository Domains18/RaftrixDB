// Package consensus implements the Raft consensus algorithm from scratch.
//
// Reference: Diego Ongaro and John Ousterhout, "In Search of an
// Understandable Consensus Algorithm (Extended Version)", 2014.
// https://raft.github.io/raft.pdf
package consensus

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/Domains18/RaftrixDB.git/config"
	"github.com/Domains18/RaftrixDB.git/internal/storage"
	"github.com/Domains18/RaftrixDB.git/pkg/types"
)

// Transport is the interface the Raft node uses to communicate with peers.
// The cluster package provides a concrete implementation over HTTP/JSON.
type Transport interface {
	AppendEntries(ctx context.Context, peer types.Peer, req *types.AppendEntriesRequest) (*types.AppendEntriesResponse, error)
	RequestVote(ctx context.Context, peer types.Peer, req *types.RequestVoteRequest) (*types.RequestVoteResponse, error)
	InstallSnapshot(ctx context.Context, peer types.Peer, req *types.InstallSnapshotRequest) (*types.InstallSnapshotResponse, error)
}

// pendingWrite tracks a client write waiting for its log entry to be committed.
type pendingWrite struct {
	index  types.LogIndex
	doneCh chan error
}

// RaftNode is a single participant in a Raft cluster.
// It manages leader election, log replication, and state machine application.
type RaftNode struct {
	mu sync.Mutex

	// ── identity ────────────────────────────────────────────────────────────
	id    types.NodeID
	peers []types.Peer // all other nodes (never includes self)

	// ── persistent state (survives restarts) ────────────────────────────────
	raftLog     *RaftLog
	currentTerm types.Term
	votedFor    types.NodeID

	// ── volatile state ──────────────────────────────────────────────────────
	state       types.NodeState
	commitIndex types.LogIndex
	lastApplied types.LogIndex
	leaderID    types.NodeID // who we think the current leader is

	// ── leader-only volatile state ──────────────────────────────────────────
	leader *leaderState

	// ── state machine ────────────────────────────────────────────────────────
	storage storage.Engine

	// ── transport layer ──────────────────────────────────────────────────────
	transport Transport

	// ── timers ───────────────────────────────────────────────────────────────
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// ── config ───────────────────────────────────────────────────────────────
	cfg *config.Config

	// ── shutdown ─────────────────────────────────────────────────────────────
	stopCh chan struct{}
	wg     sync.WaitGroup

	// ── pending client writes ─────────────────────────────────────────────────
	// Maps log index → channel that unblocks when the entry is committed.
	pending map[types.LogIndex]chan error

	// ── snapshot ─────────────────────────────────────────────────────────────
	snapshotData []byte // latest installed snapshot bytes (for lagging peers)
}

// NewRaftNode constructs a RaftNode and loads any persisted state from disk.
func NewRaftNode(
	cfg *config.Config,
	store storage.Engine,
	transport Transport,
) (*RaftNode, error) {

	peers := make([]types.Peer, len(cfg.Peers))
	for i, p := range cfg.Peers {
		peers[i] = types.Peer{ID: types.NodeID(p.ID), Addr: p.Addr}
	}

	logPath := fmt.Sprintf("%s/raft.db", cfg.Storage.DataDir)
	rl, err := newRaftLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("opening raft log: %w", err)
	}

	// Reload persisted term / votedFor.
	term, votedFor, err := rl.LoadMeta()
	if err != nil {
		return nil, fmt.Errorf("loading raft meta: %w", err)
	}

	n := &RaftNode{
		id:          types.NodeID(cfg.Node.ID),
		peers:       peers,
		raftLog:     rl,
		currentTerm: term,
		votedFor:    votedFor,
		state:       types.Follower,
		storage:     store,
		transport:   transport,
		cfg:         cfg,
		stopCh:      make(chan struct{}),
		pending:     make(map[types.LogIndex]chan error),
	}

	return n, nil
}

// ── Public API ─────────────────────────────────────────────────────────────

// Start begins the Raft event loop. Call this after wiring up the transport.
func (n *RaftNode) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	n.wg.Add(1)
	go n.run()
}

// Stop gracefully shuts the node down.
func (n *RaftNode) Stop() {
	close(n.stopCh)
	n.mu.Lock()
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.mu.Unlock()
	n.wg.Wait()
	_ = n.raftLog.Close()
}

// Propose submits a command to the cluster. It blocks until the command is
// committed by a quorum (or the context is cancelled). Returns an error if
// this node is not the leader.
func (n *RaftNode) Propose(ctx context.Context, cmd types.Command) error {
	n.mu.Lock()

	if n.state != types.Leader {
		leaderID := n.leaderID
		n.mu.Unlock()
		return &NotLeaderError{LeaderID: leaderID}
	}

	// Append to local log first.
	entry := types.LogEntry{
		Index:   n.raftLog.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: cmd,
	}
	if err := n.raftLog.Append(entry); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("appending to log: %w", err)
	}

	// Register a channel to wait for commitment.
	doneCh := make(chan error, 1)
	n.pending[entry.Index] = doneCh

	n.mu.Unlock()

	// Replicate immediately without waiting for the heartbeat tick.
	n.broadcastAppendEntries()

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.pending, entry.Index)
		n.mu.Unlock()
		return ctx.Err()
	case <-n.stopCh:
		return fmt.Errorf("node stopped")
	}
}

// IsLeader reports whether this node is currently the cluster leader.
func (n *RaftNode) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == types.Leader
}

// LeaderID returns the node ID of the last known leader (may be stale).
func (n *RaftNode) LeaderID() types.NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// ID returns this node's ID.
func (n *RaftNode) ID() types.NodeID { return n.id }

// ── RPC Handlers ──────────────────────────────────────────────────────────
// These are called by the HTTP transport when a peer contacts us.

// HandleAppendEntries processes an AppendEntries RPC from a leader.
// It is the central heartbeat + log-replication handler (§5.2, §5.3).
func (n *RaftNode) HandleAppendEntries(req *types.AppendEntriesRequest) *types.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &types.AppendEntriesResponse{
		FollowerID: n.id,
		Term:       n.currentTerm,
	}

	// Rule 1: reject if leader's term is stale.
	if req.Term < n.currentTerm {
		return resp
	}

	// A valid leader contacted us — reset election timer.
	n.resetElectionTimer()

	// Discovered a newer term: step down.
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		resp.Term = n.currentTerm
	}

	n.leaderID = req.LeaderID

	// Rule 2: consistency check — do we have the entry at prevLogIndex?
	if req.PrevLogIndex > 0 {
		lastIndex := n.raftLog.LastIndex()

		if req.PrevLogIndex > lastIndex {
			// We're missing entries; tell the leader where we are.
			resp.ConflictIndex = lastIndex + 1
			resp.ConflictTerm = 0
			return resp
		}

		prevTerm := n.raftLog.TermAt(req.PrevLogIndex)
		if prevTerm != req.PrevLogTerm {
			// Find the first index of the conflicting term so the leader
			// can skip the whole term in one round-trip.
			resp.ConflictTerm = prevTerm
			resp.ConflictIndex = req.PrevLogIndex
			for resp.ConflictIndex > n.raftLog.SnapshotIndex()+1 {
				if n.raftLog.TermAt(resp.ConflictIndex-1) != prevTerm {
					break
				}
				resp.ConflictIndex--
			}
			return resp
		}
	}

	// Rule 3 & 4: append new entries, truncating any conflicting tail.
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + types.LogIndex(i) + 1
		existingTerm := n.raftLog.TermAt(idx)

		if existingTerm != 0 && existingTerm != entry.Term {
			// Conflict — truncate from here.
			if err := n.raftLog.TruncateFrom(idx); err != nil {
				slog.Error("truncating log", "err", err)
				return resp
			}
		}

		if existingTerm == 0 {
			// Entry doesn't exist yet — append.
			if err := n.raftLog.Append(entry); err != nil {
				slog.Error("appending entry", "err", err)
				return resp
			}
		}
	}

	// Rule 5: advance commitIndex.
	if req.LeaderCommit > n.commitIndex {
		lastNew := req.PrevLogIndex + types.LogIndex(len(req.Entries))
		newCommit := req.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			go n.applyCommitted()
		}
	}

	resp.Success = true
	resp.MatchIndex = n.raftLog.LastIndex()
	return resp
}

// HandleRequestVote processes a RequestVote RPC from a candidate (§5.2).
func (n *RaftNode) HandleRequestVote(req *types.RequestVoteRequest) *types.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &types.RequestVoteResponse{
		VoterID: n.id,
		Term:    n.currentTerm,
	}

	// Stale candidate.
	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		resp.Term = n.currentTerm
	}

	// Can we vote?
	alreadyVoted := n.votedFor != "" && n.votedFor != req.CandidateID
	if alreadyVoted {
		return resp
	}

	// Is the candidate's log at least as up-to-date as ours? (§5.4.1)
	myLastIndex := n.raftLog.LastIndex()
	myLastTerm := n.raftLog.LastTerm()

	candidateUpToDate := req.LastLogTerm > myLastTerm ||
		(req.LastLogTerm == myLastTerm && req.LastLogIndex >= myLastIndex)

	if !candidateUpToDate {
		return resp
	}

	// Grant the vote.
	n.votedFor = req.CandidateID
	if err := n.raftLog.SaveMeta(n.currentTerm, n.votedFor); err != nil {
		slog.Error("saving meta after vote", "err", err)
	}
	n.resetElectionTimer()
	resp.VoteGranted = true
	return resp
}

// HandleInstallSnapshot processes an InstallSnapshot RPC from the leader.
func (n *RaftNode) HandleInstallSnapshot(req *types.InstallSnapshotRequest) *types.InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &types.InstallSnapshotResponse{
		FollowerID: n.id,
		Term:       n.currentTerm,
	}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	n.resetElectionTimer()
	n.leaderID = req.LeaderID

	// Apply the snapshot to the state machine.
	if err := n.storage.RestoreSnapshot(req.Data); err != nil {
		slog.Error("restoring snapshot", "err", err)
		return resp
	}

	// Compact the log up to the snapshot index.
	if err := n.raftLog.Compact(req.LastIncludedIndex, req.LastIncludedTerm); err != nil {
		slog.Error("compacting log after snapshot", "err", err)
		return resp
	}

	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex
	n.snapshotData = req.Data

	resp.Success = true
	return resp
}

// ── Internal event loop ────────────────────────────────────────────────────

func (n *RaftNode) run() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.state != types.Leader {
				n.mu.Unlock()
				n.startElection()
			} else {
				n.resetElectionTimer()
				n.mu.Unlock()
			}
		case <-func() <-chan time.Time {
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.heartbeatTimer != nil {
				return n.heartbeatTimer.C
			}
			return nil
		}():
			n.mu.Lock()
			if n.state == types.Leader {
				n.resetHeartbeatTimer()
				n.mu.Unlock()
				n.broadcastAppendEntries()
			} else {
				n.mu.Unlock()
			}
		}
	}
}

// startElection transitions this node to Candidate and requests votes.
func (n *RaftNode) startElection() {
	n.mu.Lock()

	n.state = types.Candidate
	n.currentTerm++
	n.votedFor = n.id
	_ = n.raftLog.SaveMeta(n.currentTerm, n.votedFor)
	n.resetElectionTimer()

	term := n.currentTerm
	lastIndex := n.raftLog.LastIndex()
	lastTerm := n.raftLog.LastTerm()
	peers := n.peers

	slog.Info("starting election", "node", n.id, "term", term)
	n.mu.Unlock()

	req := &types.RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	votes := 1 // vote for self
	voteMu := sync.Mutex{}
	quorum := len(peers)/2 + 1 // peers doesn't include self
	wonCh := make(chan struct{}, 1)

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p types.Peer) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			resp, err := n.transport.RequestVote(ctx, p, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			// Step down if we see a higher term.
			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if !resp.VoteGranted {
				return
			}

			voteMu.Lock()
			votes++
if votes >= quorum {
				select {
				case wonCh <- struct{}{}:
				default:
				}
			}
			voteMu.Unlock()
		}(peer)
	}

	// Wait for quorum or timeout.
	wg.Wait()

	select {
	case <-wonCh:
		n.becomeLeader()
	default:
		// Did not win — remain Candidate until the next election timeout.
	}
}

// becomeLeader transitions this node to Leader and sends initial heartbeats.
func (n *RaftNode) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Guard: we may have stepped down during vote gathering.
	if n.state != types.Candidate {
		return
	}

	slog.Info("became leader", "node", n.id, "term", n.currentTerm)
	n.state = types.Leader
	n.leaderID = n.id
	n.leader = newLeaderState(n.peers, n.raftLog.LastIndex())

	// Stop the election timer; start the heartbeat timer.
	n.electionTimer.Stop()
	n.resetHeartbeatTimer()

	// Append a no-op entry to commit any uncommitted entries from prior terms.
	noopEntry := types.LogEntry{
		Index:   n.raftLog.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: types.Command{Op: types.OpNoop},
	}
	_ = n.raftLog.Append(noopEntry)
}

// becomeFollower transitions the node to Follower and updates the term.
// MUST be called with n.mu held.
func (n *RaftNode) becomeFollower(term types.Term) {
	slog.Info("becoming follower", "node", n.id, "term", term)
	n.state = types.Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leader = nil
	_ = n.raftLog.SaveMeta(n.currentTerm, n.votedFor)
	n.resetElectionTimer()
}

// broadcastAppendEntries sends AppendEntries to all peers concurrently.
// Called by the leader both for heartbeats and after a new Propose().
func (n *RaftNode) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != types.Leader {
		n.mu.Unlock()
		return
	}
	peers := n.peers
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer)
	}
}

// replicateToPeer sends the appropriate AppendEntries (or InstallSnapshot)
// to a single follower, then processes its response.
func (n *RaftNode) replicateToPeer(peer types.Peer) {
	n.mu.Lock()

	if n.state != types.Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.leader.nextIndex[peer.ID]
	snapshotIdx := n.raftLog.SnapshotIndex()

	// If the peer is behind the snapshot, send a snapshot instead.
	if nextIdx <= snapshotIdx {
		data := n.snapshotData
		req := &types.InstallSnapshotRequest{
			Term:              n.currentTerm,
			LeaderID:          n.id,
			LastIncludedIndex: snapshotIdx,
			LastIncludedTerm:  n.raftLog.SnapshotTerm(),
			Data:              data,
		}
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		resp, err := n.transport.InstallSnapshot(ctx, peer, req)
		if err != nil {
			return
		}

		n.mu.Lock()
		defer n.mu.Unlock()
		if resp.Term > n.currentTerm {
			n.becomeFollower(resp.Term)
			return
		}
		if resp.Success {
			n.leader.nextIndex[peer.ID] = snapshotIdx + 1
			n.leader.matchIndex[peer.ID] = snapshotIdx
		}
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := n.raftLog.TermAt(prevLogIndex)
	lastIndex := n.raftLog.LastIndex()
	commitIndex := n.commitIndex
	term := n.currentTerm

	var entries []types.LogEntry
	if lastIndex >= nextIdx {
		var err error
		entries, err = n.raftLog.GetRange(nextIdx, lastIndex)
		if err != nil {
			n.mu.Unlock()
			return
		}
	}
	n.mu.Unlock()

	req := &types.AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	resp, err := n.transport.AppendEntries(ctx, peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != types.Leader {
		return
	}

	// Newer term found — step down immediately.
	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	if resp.Success {
		// Update match/next index.
		if resp.MatchIndex > n.leader.matchIndex[peer.ID] {
			n.leader.matchIndex[peer.ID] = resp.MatchIndex
		}
		n.leader.nextIndex[peer.ID] = n.leader.matchIndex[peer.ID] + 1

		// Check whether we can now advance commitIndex.
		newCommit := n.leader.commitCandidate(
			n.commitIndex,
			n.raftLog.LastIndex(),
			n.currentTerm,
			n.raftLog.TermAt,
		)
		if newCommit > 0 {
			n.commitIndex = newCommit
			go n.applyCommitted()
		}
	} else {
		// Log inconsistency — back up nextIndex using conflict hint.
		if resp.ConflictIndex > 0 {
			n.leader.nextIndex[peer.ID] = resp.ConflictIndex
		} else if n.leader.nextIndex[peer.ID] > 1 {
			n.leader.nextIndex[peer.ID]--
		}
	}
}

// applyCommitted applies all committed-but-not-yet-applied entries to the
// state machine and notifies any waiting Propose() callers.
func (n *RaftNode) applyCommitted() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry, err := n.raftLog.Get(n.lastApplied)
		if err != nil {
			slog.Error("reading committed entry", "index", n.lastApplied, "err", err)
			return
		}

		applyErr := n.applyCommand(entry.Command)

		// Unblock a waiting Propose() if one registered for this index.
		if ch, ok := n.pending[n.lastApplied]; ok {
			ch <- applyErr
			delete(n.pending, n.lastApplied)
		}

		// Trigger snapshot if the log has grown too large.
		if int(n.lastApplied-n.raftLog.SnapshotIndex()) >= n.cfg.Raft.SnapshotThreshold {
			go n.takeSnapshot(n.lastApplied, entry.Term)
		}
	}
}

// applyCommand applies a single command to the storage state machine.
func (n *RaftNode) applyCommand(cmd types.Command) error {
	switch cmd.Op {
	case types.OpNoop:
		return nil
	case types.OpPut:
		return n.storage.Put(cmd.Key, cmd.Value)
	case types.OpDelete:
		return n.storage.Delete(cmd.Key)
	case types.OpCAS:
		existing, err := n.storage.Get(cmd.Key)
		if err == storage.ErrKeyNotFound {
			existing = nil
		} else if err != nil {
			return err
		}
		if string(existing) != string(cmd.PrevValue) {
			return fmt.Errorf("CAS failed: value mismatch")
		}
		return n.storage.Put(cmd.Key, cmd.Value)
	default:
		return fmt.Errorf("unknown op: %s", cmd.Op)
	}
}

// takeSnapshot serialises the state machine and compacts the log.
func (n *RaftNode) takeSnapshot(index types.LogIndex, term types.Term) {
	data, err := n.storage.Snapshot()
	if err != nil {
		slog.Error("taking snapshot", "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.raftLog.Compact(index, term); err != nil {
		slog.Error("compacting log", "err", err)
		return
	}

	n.snapshotData = data
	slog.Info("snapshot taken", "node", n.id, "index", index)
}

// ── Timer helpers ──────────────────────────────────────────────────────────

// resetElectionTimer restarts the election timeout with a random duration.
// MUST be called with n.mu held.
func (n *RaftNode) resetElectionTimer() {
	min := n.cfg.Raft.ElectionTimeoutMinMs
	max := n.cfg.Raft.ElectionTimeoutMaxMs
	//nolint:gosec // random timeout does not need cryptographic randomness
	timeout := time.Duration(min+rand.Intn(max-min)) * time.Millisecond

	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(timeout)
	}
}

// resetHeartbeatTimer restarts the heartbeat ticker.
// MUST be called with n.mu held.
func (n *RaftNode) resetHeartbeatTimer() {
	interval := time.Duration(n.cfg.Raft.HeartbeatIntervalMs) * time.Millisecond
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(interval)
	} else {
		if !n.heartbeatTimer.Stop() {
			select {
			case <-n.heartbeatTimer.C:
			default:
			}
		}
		n.heartbeatTimer.Reset(interval)
	}
}

// ── Errors ─────────────────────────────────────────────────────────────────

// NotLeaderError is returned by Propose when this node is not the leader.
type NotLeaderError struct {
	LeaderID types.NodeID
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "not leader (leader unknown)"
	}
	return fmt.Sprintf("not leader, redirect to %s", e.LeaderID)
}