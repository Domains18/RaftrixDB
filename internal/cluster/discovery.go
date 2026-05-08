package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Domains18/RaftrixDB.git/pkg/types"
)

// HTTPTransport implements consensus.Transport using HTTP/JSON.
// Each Raft RPC becomes a POST request to the peer's /raft/* endpoint.
//
// This approach avoids requiring protoc / gRPC code generation while
// keeping the protocol well-structured and easy to debug with curl.
type HTTPTransport struct {
	mu      sync.RWMutex
	members map[types.NodeID]*Member

	client *http.Client
}

// NewHTTPTransport creates a transport pre-populated with the known peers.
func NewHTTPTransport(peers []types.Peer) *HTTPTransport {
	members := make(map[types.NodeID]*Member, len(peers))
	for _, p := range peers {
		members[p.ID] = NewMember(p)
	}
	return &HTTPTransport{
		members: members,
		client: &http.Client{
			Timeout: 300 * time.Millisecond,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 4,
			},
		},
	}
}

// Members returns a snapshot of all known peers and their health.
func (t *HTTPTransport) Members() []*Member {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*Member, 0, len(t.members))
	for _, m := range t.members {
		out = append(out, m)
	}
	return out
}

// ── consensus.Transport interface ─────────────────────────────────────────

func (t *HTTPTransport) AppendEntries(
	ctx context.Context,
	peer types.Peer,
	req *types.AppendEntriesRequest,
) (*types.AppendEntriesResponse, error) {
	var resp types.AppendEntriesResponse
	if err := t.rpc(ctx, peer, "/raft/append-entries", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (t *HTTPTransport) RequestVote(
	ctx context.Context,
	peer types.Peer,
	req *types.RequestVoteRequest,
) (*types.RequestVoteResponse, error) {
	var resp types.RequestVoteResponse
	if err := t.rpc(ctx, peer, "/raft/request-vote", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (t *HTTPTransport) InstallSnapshot(
	ctx context.Context,
	peer types.Peer,
	req *types.InstallSnapshotRequest,
) (*types.InstallSnapshotResponse, error) {
	var resp types.InstallSnapshotResponse
	if err := t.rpc(ctx, peer, "/raft/install-snapshot", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ── internal ──────────────────────────────────────────────────────────────

func (t *HTTPTransport) rpc(ctx context.Context, peer types.Peer, path string, body, out any) error {
	url := fmt.Sprintf("http://%s%s", peer.Addr, path)

	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		t.recordFailure(peer.ID)
		return fmt.Errorf("rpc %s to %s: %w", path, peer.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.recordFailure(peer.ID)
		return fmt.Errorf("rpc %s to %s: status %d", path, peer.ID, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding response from %s: %w", peer.ID, err)
	}

	t.recordSuccess(peer.ID)
	return nil
}

func (t *HTTPTransport) recordSuccess(id types.NodeID) {
	t.mu.RLock()
	m, ok := t.members[id]
	t.mu.RUnlock()
	if ok {
		m.RecordSuccess()
	}
}

func (t *HTTPTransport) recordFailure(id types.NodeID) {
	t.mu.RLock()
	m, ok := t.members[id]
	t.mu.RUnlock()
	if ok {
		m.RecordFailure()
	}
}