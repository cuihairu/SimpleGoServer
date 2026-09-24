package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Sentinel errors keep HTTP vocabulary out of this layer: the handler
// maps each one to exactly one status code via errors.Is, so both
// sides evolve independently.
var (
	ErrNotFound      = errors.New("key not found")
	ErrKeyInvalid    = errors.New("key must be 1-128 printable non-space ASCII bytes")
	ErrValueInvalid  = errors.New("body must be exactly one valid JSON value")
	ErrValueTooLarge = errors.New("value exceeds the configured size limit")
)

const (
	maxKeyLen = 128
	// Same single-entry ceiling as the TCP service's frames: one
	// decision, stated once; -max-bytes can override it for demos.
	defaultMaxValueBytes = 1 << 20
)

// Service holds the business rules — the only layer that knows what a
// valid entry is. The handler knows HTTP, the store knows bytes-in-a-
// map. ctx flows through every call so a remote Store implementation
// can honor deadlines and cancellation.
type Service struct {
	store    Store
	maxValue int64
}

func NewService(store Store, maxValue int64) *Service {
	return &Service{store: store, maxValue: maxValue}
}

func validKey(key string) bool {
	if len(key) == 0 || len(key) > maxKeyLen {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < '!' || key[i] > '~' { // printable, no spaces
			return false
		}
	}
	return true
}

func (s *Service) Get(ctx context.Context, key string) (Value, error) {
	if !validKey(key) {
		return nil, ErrKeyInvalid
	}
	v, ok := s.store.Get(ctx, key)
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

// Put reads at most maxValue+1 bytes: an oversized body fails with
// ErrValueTooLarge instead of buffering unbounded input in memory.
// The value must be exactly one JSON value — validated here, once —
// and is passed down as RawMessage so no layer ever re-encodes it.
func (s *Service) Put(ctx context.Context, key string, body io.Reader) (bool, error) {
	if !validKey(key) {
		return false, ErrKeyInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(body, s.maxValue+1))
	if err != nil {
		return false, fmt.Errorf("read body: %w", err)
	}
	if int64(len(raw)) > s.maxValue {
		return false, ErrValueTooLarge
	}
	if !json.Valid(raw) {
		return false, ErrValueInvalid
	}
	return s.store.Put(ctx, key, Value(raw))
}

func (s *Service) Delete(ctx context.Context, key string) (bool, error) {
	if !validKey(key) {
		return false, ErrKeyInvalid
	}
	return s.store.Delete(ctx, key), nil
}
