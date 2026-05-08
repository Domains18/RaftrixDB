package cluster

import (
	"sync"
	"time"

	"github.com/Domains18/RaftrixDB.git/pkg/types"
)

// PeerState tracks the observed health of a remote cluster member.
type PeerState int

const (
	PeerAlive       PeerState = iota
	PeerUnreachable           // missed at least one RPC
	PeerDead                  // missed enough RPCs to be declared dead
)

// Member represents a remote Raft peer and its current health status.
type Member struct {
	mu sync.RWMutex

	Peer     types.Peer
	state    PeerState
	lastSeen time.Time
	missedRPCs int
}

// NewMember creates a Member for the given peer, initially considered alive.
func NewMember(peer types.Peer) *Member {
	return &Member{
		Peer:     peer,
		state:    PeerAlive,
		lastSeen: time.Now(),
	}
}

// RecordSuccess marks the peer as reachable and resets the miss counter.
func (m *Member) RecordSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = PeerAlive
	m.lastSeen = time.Now()
	m.missedRPCs = 0
}

// RecordFailure increments the miss counter and downgrades the peer state.
func (m *Member) RecordFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.missedRPCs++
	switch {
	case m.missedRPCs >= 5:
		m.state = PeerDead
	case m.missedRPCs >= 2:
		m.state = PeerUnreachable
	}
}

// IsAlive returns true if the peer responded to a recent RPC.
func (m *Member) IsAlive() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state == PeerAlive
}

// State returns the peer's current health state.
func (m *Member) State() PeerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// LastSeen returns the time of the most recent successful RPC.
func (m *Member) LastSeen() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSeen
}