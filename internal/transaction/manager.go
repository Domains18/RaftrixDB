package transaction

import (
	"context"
	"fmt"

	"github.com/Domains18/RaftrixDB.git/internal/consensus"
)

// Proposer is the subset of consensus.RaftNode used by the Manager.
type Proposer interface {
	Propose(ctx context.Context, cmd interface{ GetCommand() interface{} }) error
}

// Manager commits transactions through the Raft log.
type Manager struct {
	node *consensus.RaftNode
}

// NewManager creates a Manager backed by the given RaftNode.
func NewManager(node *consensus.RaftNode) *Manager {
	return &Manager{node: node}
}

// Commit proposes all operations in txn to the cluster sequentially.
// If any operation fails the remaining operations are NOT rolled back —
// callers should use CAS operations to guard against partial failure.
//
// True atomic multi-key transactions would require a single compound log
// entry; this is tracked as a future enhancement.
func (m *Manager) Commit(ctx context.Context, txn *Txn) error {
	if txn.Len() == 0 {
		return nil
	}
	for i, op := range txn.Ops() {
		if err := m.node.Propose(ctx, op.Command); err != nil {
			return fmt.Errorf("txn op[%d] (%s %q): %w", i, op.Command.Op, op.Command.Key, err)
		}
	}
	return nil
}