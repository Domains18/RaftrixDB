package consensus

import "github.com/Domains18/RaftrixDB.git/pkg/types"

// leaderState holds the volatile state only valid while this node is Leader.
// It is re-initialised fresh on every election win (§5.3 of the Raft paper).
type leaderState struct {
	// nextIndex[peer] = index of the next log entry to send to that peer.
	// Initialised to leader's lastIndex + 1.
	nextIndex map[types.NodeID]types.LogIndex

	// matchIndex[peer] = highest log index known to be replicated on peer.
	// Initialised to 0, increases monotonically.
	matchIndex map[types.NodeID]types.LogIndex
}

func newLeaderState(peers []types.Peer, lastIndex types.LogIndex) *leaderState {
	ls := &leaderState{
		nextIndex:  make(map[types.NodeID]types.LogIndex, len(peers)),
		matchIndex: make(map[types.NodeID]types.LogIndex, len(peers)),
	}
	for _, p := range peers {
		ls.nextIndex[p.ID] = lastIndex + 1
		ls.matchIndex[p.ID] = 0
	}
	return ls
}

// commitCandidate returns the highest N such that:
//   - N > currentCommit
//   - a majority of matchIndex[i] >= N
//   - log[N].Term == currentTerm   (Raft safety: §5.4.2)
//
// Returns 0 if no such N exists.
func (ls *leaderState) commitCandidate(
	currentCommit types.LogIndex,
	lastIndex types.LogIndex,
	currentTerm types.Term,
	termAt func(types.LogIndex) types.Term,
) types.LogIndex {
	majority := (len(ls.matchIndex)+1)/2 + 1 // +1 for the leader itself

	for n := lastIndex; n > currentCommit; n-- {
		if termAt(n) != currentTerm {
			continue // only commit entries from the current term
		}
		count := 1 // leader already has it
		for _, mi := range ls.matchIndex {
			if mi >= n {
				count++
			}
		}
		if count >= majority {
			return n
		}
	}
	return 0
}