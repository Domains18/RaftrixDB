package consensus

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"

	"github.com/Domains18/RaftrixDB.git/pkg/types"
)

var (
	logBucket  = []byte("raft_log")
	metaBucket = []byte("raft_meta")
	metaKey    = []byte("state")

	// ErrLogEntryNotFound is returned when a log index does not exist.
	ErrLogEntryNotFound = errors.New("log entry not found")
)

// RaftLog manages the persistent Raft log and durable metadata
// (currentTerm + votedFor) using a BoltDB database.
//
// Index 0 is reserved; real entries start at index 1.
// After a snapshot the first valid index is snapshotIndex + 1.
type RaftLog struct {
	db *bolt.DB

	// snapshotIndex / snapshotTerm represent the last entry included
	// in the most recent snapshot (log compaction point).
	snapshotIndex types.LogIndex
	snapshotTerm  types.Term
}

// newRaftLog opens (or creates) the Raft log BoltDB database.
func newRaftLog(path string) (*RaftLog, error) {
	if err := os.MkdirAll(extractDir(path), 0o755); err != nil {
		return nil, fmt.Errorf("creating log dir: %w", err)
	}

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("opening raft log %q: %w", path, err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(logBucket); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(metaBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	rl := &RaftLog{db: db}

	// Reload snapshot metadata persisted from a previous run.
	_ = db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(metaBucket).Get([]byte("snapshot"))
		if data == nil {
			return nil
		}
		var snap struct {
			Index types.LogIndex `json:"index"`
			Term  types.Term     `json:"term"`
		}
		if err := json.Unmarshal(data, &snap); err == nil {
			rl.snapshotIndex = snap.Index
			rl.snapshotTerm = snap.Term
		}
		return nil
	})

	return rl, nil
}

// -----------------------------------------------------------------------
// Term / VotedFor persistence
// -----------------------------------------------------------------------

// SaveMeta atomically persists currentTerm and votedFor to disk.
func (rl *RaftLog) SaveMeta(term types.Term, votedFor types.NodeID) error {
	return rl.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(struct {
			Term     types.Term   `json:"term"`
			VotedFor types.NodeID `json:"voted_for"`
		}{term, votedFor})
		if err != nil {
			return err
		}
		return tx.Bucket(metaBucket).Put(metaKey, data)
	})
}

// LoadMeta restores currentTerm and votedFor from disk.
// Returns zero values if no metadata has been persisted yet.
func (rl *RaftLog) LoadMeta() (term types.Term, votedFor types.NodeID, err error) {
	err = rl.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(metaBucket).Get(metaKey)
		if data == nil {
			return nil // first boot
		}
		var s struct {
			Term     types.Term   `json:"term"`
			VotedFor types.NodeID `json:"voted_for"`
		}
		if e := json.Unmarshal(data, &s); e != nil {
			return e
		}
		term = s.Term
		votedFor = s.VotedFor
		return nil
	})
	return
}

// -----------------------------------------------------------------------
// Log entry operations
// -----------------------------------------------------------------------

// Append adds an entry to the log. The entry's Index must be exactly
// LastIndex() + 1.
func (rl *RaftLog) Append(entry types.LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return rl.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(logBucket).Put(indexKey(entry.Index), data)
	})
}

// AppendBatch appends multiple entries in a single BoltDB transaction.
func (rl *RaftLog) AppendBatch(entries []types.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return rl.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(logBucket)
		for _, e := range entries {
			data, err := json.Marshal(e)
			if err != nil {
				return err
			}
			if err := bkt.Put(indexKey(e.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

// Get retrieves a single log entry by index.
func (rl *RaftLog) Get(index types.LogIndex) (types.LogEntry, error) {
	var entry types.LogEntry
	err := rl.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(logBucket).Get(indexKey(index))
		if data == nil {
			return ErrLogEntryNotFound
		}
		return json.Unmarshal(data, &entry)
	})
	return entry, err
}

// GetRange returns entries in [from, to] inclusive.
func (rl *RaftLog) GetRange(from, to types.LogIndex) ([]types.LogEntry, error) {
	var entries []types.LogEntry
	err := rl.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(logBucket)
		for i := from; i <= to; i++ {
			data := bkt.Get(indexKey(i))
			if data == nil {
				break
			}
			var e types.LogEntry
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
			entries = append(entries, e)
		}
		return nil
	})
	return entries, err
}

// TruncateFrom removes all entries with index >= from.
// Used when a follower discovers log conflicts with the leader.
func (rl *RaftLog) TruncateFrom(from types.LogIndex) error {
	return rl.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(logBucket)
		c := bkt.Cursor()
		for k, _ := c.Seek(indexKey(from)); k != nil; k, _ = c.Next() {
			if err := bkt.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// LastIndex returns the index of the last entry in the log.
// Returns snapshotIndex if the log is empty (all entries compacted).
func (rl *RaftLog) LastIndex() types.LogIndex {
	var last types.LogIndex = rl.snapshotIndex
	_ = rl.db.View(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(logBucket).Cursor().Last()
		if k != nil {
			last = bytesToIndex(k)
		}
		return nil
	})
	return last
}

// LastTerm returns the term of the last log entry.
func (rl *RaftLog) LastTerm() types.Term {
	lastIdx := rl.LastIndex()
	if lastIdx == 0 {
		return 0
	}
	if lastIdx == rl.snapshotIndex {
		return rl.snapshotTerm
	}
	e, err := rl.Get(lastIdx)
	if err != nil {
		return rl.snapshotTerm
	}
	return e.Term
}

// TermAt returns the term of the entry at index.
// Returns snapshotTerm for the snapshot boundary index.
func (rl *RaftLog) TermAt(index types.LogIndex) types.Term {
	if index == 0 {
		return 0
	}
	if index == rl.snapshotIndex {
		return rl.snapshotTerm
	}
	e, err := rl.Get(index)
	if err != nil {
		return 0
	}
	return e.Term
}

// -----------------------------------------------------------------------
// Snapshot / compaction
// -----------------------------------------------------------------------

// Compact discards all log entries up to and including snapshotIndex,
// and records the snapshot boundary so future LastIndex/LastTerm calls
// remain correct.
func (rl *RaftLog) Compact(snapshotIndex types.LogIndex, snapshotTerm types.Term) error {
	return rl.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(logBucket)
		c := bkt.Cursor()
		// Delete everything up to and including snapshotIndex.
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			idx := bytesToIndex(k)
			if idx > snapshotIndex {
				break
			}
			if err := bkt.Delete(k); err != nil {
				return err
			}
		}

		// Persist snapshot boundary.
		data, _ := json.Marshal(struct {
			Index types.LogIndex `json:"index"`
			Term  types.Term     `json:"term"`
		}{snapshotIndex, snapshotTerm})
		return tx.Bucket(metaBucket).Put([]byte("snapshot"), data)
	})
}

func (rl *RaftLog) SnapshotIndex() types.LogIndex { return rl.snapshotIndex }
func (rl *RaftLog) SnapshotTerm() types.Term      { return rl.snapshotTerm }

func (rl *RaftLog) Close() error { return rl.db.Close() }

// -----------------------------------------------------------------------
// Binary key encoding — big-endian uint64 so BoltDB's B-tree orders
// entries by index correctly.
// -----------------------------------------------------------------------

func indexKey(idx types.LogIndex) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(idx))
	return b
}

func bytesToIndex(b []byte) types.LogIndex {
	return types.LogIndex(binary.BigEndian.Uint64(b))
}

func extractDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[:i]
		}
	}
	return "."
}