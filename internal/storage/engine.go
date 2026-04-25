package storage

import "errors"

// ErrKeyNotFound is returned when a key does not exist in the store.
var ErrKeyNotFound = errors.New("key not found")

// Engine is the interface every storage backend must implement.
// It represents the state machine that Raft log entries are applied to.
type Engine interface {
	// Get retrieves the value for key. Returns ErrKeyNotFound if absent.
	Get(key string) ([]byte, error)

	// Put stores a key-value pair, overwriting any existing value.
	Put(key string, value []byte) error

	// Delete removes a key. A no-op if the key does not exist.
	Delete(key string) error

	// Scan returns all key-value pairs whose key starts with prefix.
	// An empty prefix returns all keys.
	Scan(prefix string) (map[string][]byte, error)

	// Snapshot serialises the entire state machine into a byte slice.
	// Used by Raft to compact the log and send state to lagging followers.
	Snapshot() ([]byte, error)

	// RestoreSnapshot replaces the entire state machine with the
	// data produced by a previous Snapshot call.
	RestoreSnapshot(data []byte) error

	// Close releases any resources held by the engine.
	Close() error
}