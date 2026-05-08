package cluster

import (
	"log/slog"
	"sync"
	"time"

	"github.com/Domains18/RaftrixDB.git/pkg/types"
)

const (
	checkInterval     = 5 * time.Second
	deadThreshold     = 30 * time.Second
)

// HeartbeatTracker periodically inspects the last-seen timestamps of all
// peers and logs state changes. The actual liveness data comes from the
// transport layer (RecordSuccess / RecordFailure calls after every RPC).
type HeartbeatTracker struct {
	mu      sync.Mutex
	members []*Member
	stopCh  chan struct{}
}

// NewHeartbeatTracker creates a tracker for the given member list.
func NewHeartbeatTracker(members []*Member) *HeartbeatTracker {
	return &HeartbeatTracker{
		members: members,
		stopCh:  make(chan struct{}),
	}
}

// Start launches the background health-check goroutine.
func (h *HeartbeatTracker) Start() {
	go h.run()
}

// Stop shuts the background goroutine down.
func (h *HeartbeatTracker) Stop() {
	close(h.stopCh)
}

// LivePeers returns the IDs of currently reachable peers.
func (h *HeartbeatTracker) LivePeers() []types.NodeID {
	h.mu.Lock()
	defer h.mu.Unlock()
	var live []types.NodeID
	for _, m := range h.members {
		if m.IsAlive() {
			live = append(live, m.Peer.ID)
		}
	}
	return live
}

func (h *HeartbeatTracker) run() {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.check()
		}
	}
}

func (h *HeartbeatTracker) check() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, m := range h.members {
		age := time.Since(m.LastSeen())
		state := m.State()

		if age > deadThreshold && state != PeerDead {
			slog.Warn("peer appears dead",
				"peer", m.Peer.ID,
				"last_seen_ago", age.Round(time.Second))
		}
	}
}