package storage

import (
	"encoding/json"
	"strings"
	"sync"
)

// MemoryEngine is a thread-safe in-memory key-value store.
// Useful for testing and single-node development runs.
type MemoryEngine struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryEngine creates a new, empty in-memory engine.
func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{data: make(map[string][]byte)}
}

func (m *MemoryEngine) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	result := make([]byte, len(v))
	copy(result, v)
	return result, nil
}

func (m *MemoryEngine) Put(key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	v := make([]byte, len(value))
	copy(v, value)
	m.data[key] = v
	return nil
}

func (m *MemoryEngine) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *MemoryEngine) Scan(prefix string) (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]byte)
	for k, v := range m.data {
		if strings.HasPrefix(k, prefix) {
			cp := make([]byte, len(v))
			copy(cp, v)
			result[k] = cp
		}
	}
	return result, nil
}

func (m *MemoryEngine) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return json.Marshal(m.data)
}

func (m *MemoryEngine) RestoreSnapshot(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	newData := make(map[string][]byte)
	if err := json.Unmarshal(data, &newData); err != nil {
		return err
	}
	m.data = newData
	return nil
}

func (m *MemoryEngine) Close() error { return nil }