package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	bolt "go.etcd.io/bbolt"
)

var kvBucket = []byte("kv")

// BoltEngine is a persistent key-value store backed by BoltDB (bbolt).
// It is ACID-compliant and survives process crashes.
type BoltEngine struct {
	db *bolt.DB
}

// NewBoltEngine opens (or creates) a BoltDB database at the given path.
func NewBoltEngine(path string) (*BoltEngine, error) {
	if err := os.MkdirAll(extractDir(path), 0o755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("opening bolt db %q: %w", path, err)
	}

	// Ensure the KV bucket exists.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(kvBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &BoltEngine{db: db}, nil
}

func (b *BoltEngine) Get(key string) ([]byte, error) {
	var value []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(kvBucket).Get([]byte(key))
		if v == nil {
			return ErrKeyNotFound
		}
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	return value, err
}

func (b *BoltEngine) Put(key string, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(kvBucket).Put([]byte(key), value)
	})
}

func (b *BoltEngine) Delete(key string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(kvBucket).Delete([]byte(key))
	})
}

func (b *BoltEngine) Scan(prefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(kvBucket).ForEach(func(k, v []byte) error {
			if strings.HasPrefix(string(k), prefix) {
				cp := make([]byte, len(v))
				copy(cp, v)
				result[string(k)] = cp
			}
			return nil
		})
	})
	return result, err
}

func (b *BoltEngine) Snapshot() ([]byte, error) {
	data := make(map[string][]byte)
	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(kvBucket).ForEach(func(k, v []byte) error {
			cp := make([]byte, len(v))
			copy(cp, v)
			data[string(k)] = cp
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(data)
}

func (b *BoltEngine) RestoreSnapshot(data []byte) error {
	var m map[string][]byte
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		// Drop and recreate the bucket for a clean restore.
		if err := tx.DeleteBucket(kvBucket); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}
		bkt, err := tx.CreateBucket(kvBucket)
		if err != nil {
			return err
		}
		for k, v := range m {
			if err := bkt.Put([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltEngine) Close() error { return b.db.Close() }

// extractDir strips the filename from path and returns the directory portion.
func extractDir(path string) string {
	idx := strings.LastIndexByte(path, '/')
	if idx < 0 {
		return "."
	}
	return path[:idx]
}