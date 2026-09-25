package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestServicePutValidation(t *testing.T) {
	s := NewService(NewMemoryStore(), 64)
	ctx := context.Background()

	cases := []struct {
		name string
		key  string
		body string
		want error
	}{
		{"empty key", "", `"v"`, ErrKeyInvalid},
		{"key with space", "a b", `"v"`, ErrKeyInvalid},
		{"key over 128 bytes", strings.Repeat("k", 129), `"v"`, ErrKeyInvalid},
		{"non-JSON body", "k", `not json`, ErrValueInvalid},
		{"two JSON values", "k", `1 2`, ErrValueInvalid},
		{"body over limit", "k", `"` + strings.Repeat("x", 64) + `"`, ErrValueTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := s.Put(ctx, c.key, strings.NewReader(c.body))
			if !errors.Is(err, c.want) {
				t.Fatalf("Put(%q, %q) err = %v, want %v", c.key, c.body, err, c.want)
			}
		})
	}

	// exactly at the limit is fine — the boundary belongs to the test
	created, err := s.Put(ctx, "k", strings.NewReader(`"`+strings.Repeat("x", 62)+`"`))
	if err != nil || !created {
		t.Fatalf("Put at limit: created=%v err=%v", created, err)
	}
}

func TestServiceGetDelete(t *testing.T) {
	s := NewService(NewMemoryStore(), defaultMaxValueBytes)
	ctx := context.Background()

	if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on empty store: %v, want ErrNotFound", err)
	}
	if _, err := s.Get(ctx, "bad key"); !errors.Is(err, ErrKeyInvalid) {
		t.Fatalf("Get with bad key: %v, want ErrKeyInvalid", err)
	}
	if _, err := s.Delete(ctx, "bad key"); !errors.Is(err, ErrKeyInvalid) {
		t.Fatalf("Delete with bad key: %v, want ErrKeyInvalid", err)
	}
	if _, err := s.Put(ctx, "k", strings.NewReader(`{"n":1}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, err := s.Get(ctx, "k")
	if err != nil || string(v) != `{"n":1}` {
		t.Fatalf("Get: v=%s err=%v", v, err)
	}
	deleted, err := s.Delete(ctx, "k")
	if err != nil || !deleted {
		t.Fatalf("Delete: deleted=%v err=%v", deleted, err)
	}
	if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete: %v, want ErrNotFound", err)
	}
}

// failingStore answers every call with a non-sentinel error, standing
// in for a broken remote backend: the service must pass the error
// through unwrapped into a sentinel, and the handler must answer 500.
type failingStore struct{ Store }

func (failingStore) Get(context.Context, string) (Value, bool) { return nil, false }
func (failingStore) Put(context.Context, string, Value) (bool, error) {
	return false, errors.New("connection refused")
}
func (failingStore) Delete(context.Context, string) bool { return false }

func TestServicePropagatesStoreErrors(t *testing.T) {
	s := NewService(failingStore{}, defaultMaxValueBytes)
	_, err := s.Put(context.Background(), "k", strings.NewReader(`1`))
	if err == nil || errors.Is(err, ErrValueInvalid) || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("Put through failing store: %v", err)
	}
}

// errReader fails mid-read: the read error must surface wrapped, not be
// mistaken for an empty or a short body.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read boom") }

func TestServicePutReadError(t *testing.T) {
	s := NewService(NewMemoryStore(), defaultMaxValueBytes)
	_, err := s.Put(context.Background(), "k", errReader{})
	if err == nil || !strings.Contains(err.Error(), "read body: read boom") {
		t.Fatalf("Put with a failing body reader: %v", err)
	}
}
