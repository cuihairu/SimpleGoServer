package main

import (
	"context"
	"encoding/json"
	"sync"
)

// Value is one stored entry. json.RawMessage keeps the caller's bytes
// verbatim — validated once on the way in, never re-encoded. This is
// the same double-encoding trap the TCP service hit (its 1MiB Call
// cost 42ms until RawMessage passthrough landed, see docs/Benchmark.md).
type Value = json.RawMessage

// Store is the persistence seam. Everything above it (service,
// handler) is storage-agnostic: a Redis or Postgres backend must slot
// in without touching any other layer — that is exactly what the
// tests exploit with stub stores.
type Store interface {
	Get(ctx context.Context, key string) (Value, bool)
	Put(ctx context.Context, key string, v Value) (created bool, err error)
	Delete(ctx context.Context, key string) (deleted bool)
}

// MemoryStore is the smallest correct Store: an RWMutex around a map.
// RWMutex over sync.Map is a deliberate pick — keys here are written
// as often as read, and sync.Map only pays off for read-mostly
// workloads with disjoint key sets.
type MemoryStore struct {
	mu    sync.RWMutex
	items map[string]Value
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{items: make(map[string]Value)}
}

func (m *MemoryStore) Get(_ context.Context, key string) (Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.items[key]
	return v, ok
}

// Put stores v and reports whether the key is new. The caller hands
// over ownership: v is never mutated afterwards, so no defensive copy
// is needed under the lock.
func (m *MemoryStore) Put(_ context.Context, key string, v Value) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.items[key]
	m.items[key] = v
	return !exists, nil
}

func (m *MemoryStore) Delete(_ context.Context, key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[key]; !ok {
		return false
	}
	delete(m.items, key)
	return true
}
