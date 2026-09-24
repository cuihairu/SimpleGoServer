package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestMemoryStoreRoundTrip(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	if _, ok := s.Get(ctx, "a"); ok {
		t.Fatal("empty store answered Get with a value")
	}
	created, err := s.Put(ctx, "a", Value(`1`))
	if err != nil || !created {
		t.Fatalf("first Put: created=%v err=%v", created, err)
	}
	created, err = s.Put(ctx, "a", Value(`2`))
	if err != nil || created {
		t.Fatalf("overwrite Put: created=%v err=%v", created, err)
	}
	v, ok := s.Get(ctx, "a")
	if !ok || string(v) != `2` {
		t.Fatalf("Get after overwrite: ok=%v v=%s", ok, v)
	}
	if s.Delete(ctx, "a") != true {
		t.Fatal("Delete of an existing key reported false")
	}
	if s.Delete(ctx, "a") != false {
		t.Fatal("Delete of a missing key reported true")
	}
	if _, ok := s.Get(ctx, "a"); ok {
		t.Fatal("Get found the key after Delete")
	}
}

// The point of the concurrency test is not the assertions — it is to
// give `go test -race` interleaved Put/Get/Delete traffic on shared
// keys. Run with -race; without it the test proves very little.
// Writers keep one private key with a checkable full lifecycle, and
// hammer shared keys with put/delete only: between a Put and a Get on
// a shared key another writer's Delete may land, so a "must be there"
// assertion there would be testing luck, not the lock.
func TestMemoryStoreConcurrentAccess(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	const writers, perWriter = 8, 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			private := fmt.Sprintf("p%d", w)
			for i := 0; i < perWriter; i++ {
				if created, err := s.Put(ctx, private, Value(`"v"`)); err != nil || !created {
					t.Errorf("private put #%d: created=%v err=%v", i, created, err)
					return
				}
				if v, ok := s.Get(ctx, private); !ok || string(v) != `"v"` {
					t.Errorf("private get #%d: ok=%v v=%s", i, ok, v)
					return
				}
				if !s.Delete(ctx, private) {
					t.Errorf("private delete #%d: key missing", i)
					return
				}
				shared := fmt.Sprintf("k%d", (w+i)%4)
				_, _ = s.Put(ctx, shared, Value(`"v"`))
				s.Delete(ctx, shared)
			}
		}(w)
	}
	wg.Wait()
}
