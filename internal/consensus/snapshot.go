package consensus

// snapshot.go — re-exports helpers used by raft.go for snapshot lifecycle.
// The main snapshot logic lives in raft.go (takeSnapshot, HandleInstallSnapshot).
// This file exists to document the snapshot flow and expose the current
// snapshot data for the API server to use when seeding lagging peers.

// SnapshotData returns the raw bytes of the most recently taken snapshot.
// Returns nil if no snapshot has been taken yet.
func (n *RaftNode) SnapshotData() []byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotData
}

// SnapshotIndex returns the log index through which the last snapshot was taken.
func (n *RaftNode) SnapshotIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return uint64(n.raftLog.SnapshotIndex())
}

// CommitIndex returns the highest log index known to be committed.
func (n *RaftNode) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return uint64(n.commitIndex)
}

// LastApplied returns the highest log index that has been applied to the
// state machine. Useful for clients that need linearizable reads.
func (n *RaftNode) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return uint64(n.lastApplied)
}