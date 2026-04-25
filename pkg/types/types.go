package types

// NodeID uniquely identifies a node in the cluster.
type NodeID string

// Term is a monotonically increasing election term.
type Term uint64

// LogIndex is the 1-based position of an entry in the Raft log.
type LogIndex uint64

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	Follower  NodeState = iota // default state
	Candidate                  // seeking election
	Leader                     // drives replication
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// OpType classifies a command applied to the state machine.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpDelete OpType = "DELETE"
	OpCAS    OpType = "CAS"  // compare-and-swap
	OpNoop   OpType = "NOOP" // heartbeat / leader no-op
)

// Command is the payload stored inside a LogEntry.
type Command struct {
	Op        OpType `json:"op"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	PrevValue []byte `json:"prev_value,omitempty"` // for CAS
}

// LogEntry is a single record in the Raft log.
type LogEntry struct {
	Index   LogIndex `json:"index"`
	Term    Term     `json:"term"`
	Command Command  `json:"command"`
}

// Peer is a remote node's address information.
type Peer struct {
	ID   NodeID `yaml:"id"   json:"id"`
	Addr string `yaml:"addr" json:"addr"`
}

// AppendEntriesRequest is sent by the leader to replicate log entries
// and serve as heartbeats (Entries == nil).
type AppendEntriesRequest struct {
	Term         Term       `json:"term"`
	LeaderID     NodeID     `json:"leader_id"`
	PrevLogIndex LogIndex   `json:"prev_log_index"`
	PrevLogTerm  Term       `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit LogIndex   `json:"leader_commit"`
}

// AppendEntriesResponse is the follower's reply.
type AppendEntriesResponse struct {
	Term        Term     `json:"term"`
	Success     bool     `json:"success"`
	FollowerID  NodeID   `json:"follower_id"`
	MatchIndex  LogIndex `json:"match_index"` // highest index follower now has
	// ConflictIndex helps the leader quickly back up on rejection.
	ConflictIndex LogIndex `json:"conflict_index,omitempty"`
	ConflictTerm  Term     `json:"conflict_term,omitempty"`
}

// RequestVoteRequest is sent by a candidate seeking election.
type RequestVoteRequest struct {
	Term         Term     `json:"term"`
	CandidateID  NodeID   `json:"candidate_id"`
	LastLogIndex LogIndex `json:"last_log_index"`
	LastLogTerm  Term     `json:"last_log_term"`
}

// RequestVoteResponse is the peer's reply.
type RequestVoteResponse struct {
	Term        Term   `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	VoterID     NodeID `json:"voter_id"`
}

// InstallSnapshotRequest transfers a snapshot to a lagging follower.
type InstallSnapshotRequest struct {
	Term              Term     `json:"term"`
	LeaderID          NodeID   `json:"leader_id"`
	LastIncludedIndex LogIndex `json:"last_included_index"`
	LastIncludedTerm  Term     `json:"last_included_term"`
	Data              []byte   `json:"data"`
}

// InstallSnapshotResponse is the follower's reply.
type InstallSnapshotResponse struct {
	Term       Term   `json:"term"`
	FollowerID NodeID `json:"follower_id"`
	Success    bool   `json:"success"`
}

// ProposeResult is returned to the client after a write is committed.
type ProposeResult struct {
	Value []byte // populated for CAS reads
}