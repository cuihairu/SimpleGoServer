package balancer

import (
	"testing"

	"github.com/cuihairu/simplegoserver/pkg"
)

// loadableBackend extends FakeBackend with a live load figure so it can
// satisfy the LoadAware contract the adaptive balancer requires.
type loadableBackend struct {
	*FakeBackend
	load float64
}

func (l *loadableBackend) Load() float64 { return l.load }

// probeBackendContract pins the registration contract every Balancer must
// honour, regardless of strategy: dedup on Register, idempotent
// Unregister, Size telling the truth, Iterate visiting each member once,
// and a removed backend never coming back out of Next. The weighted_random
// Unregister bug (removing the first element instead of the match) and the
// ip_hashing one (size drifting negative on unknown backends) were both
// violations of this contract, so it runs against every implementation.
func probeBackendContract[T pkg.Backend](t *testing.T, name string, newBalancer func() pkg.Balancer[T], mk func(id string) T) {
	t.Helper()

	b := newBalancer()
	a, c := mk("a"), mk("c")
	for _, backend := range []T{a, c} {
		if err := b.Register(backend); err != nil {
			t.Fatalf("%s: Register(%s): %v", name, backend.Id(), err)
		}
	}
	if b.Size() != 2 {
		t.Fatalf("%s: Size() = %d, want 2", name, b.Size())
	}

	// duplicate Register must not grow the set (ip_hashing used to stack a
	// second ring of virtual nodes and inflate its counter)
	if err := b.Register(mk("a")); err != nil {
		t.Fatalf("%s: duplicate Register: %v", name, err)
	}
	if b.Size() != 2 {
		t.Fatalf("%s: Size() = %d after duplicate Register, want 2", name, b.Size())
	}

	visited := map[string]int{}
	b.Iterate(func(backend T) bool {
		visited[backend.Id()]++
		return true
	})
	if len(visited) != 2 || visited["a"] != 1 || visited["c"] != 1 {
		t.Fatalf("%s: Iterate visited %v, want exactly one visit per backend", name, visited)
	}

	// Unregister of an unknown backend must be a no-op, not move the
	// counters
	if err := b.Unregister(mk("ghost")); err != nil {
		t.Fatalf("%s: Unregister(ghost): %v", name, err)
	}
	if b.Size() != 2 {
		t.Fatalf("%s: Size() = %d after Unregister(ghost), want 2", name, b.Size())
	}

	if err := b.Unregister(a); err != nil {
		t.Fatalf("%s: Unregister(a): %v", name, err)
	}
	if b.Size() != 1 {
		t.Fatalf("%s: Size() = %d after Unregister(a), want 1", name, b.Size())
	}
	for i := 0; i < 32; i++ {
		next, err := b.Next("key")
		if err != nil {
			t.Fatalf("%s: Next: %v", name, err)
		}
		if next.Id() != "c" {
			t.Fatalf("%s: Next() = %s after a was removed, want only c", name, next.Id())
		}
	}

	if err := b.Register(a); err != nil {
		t.Fatalf("%s: re-Register(a): %v", name, err)
	}
	if b.Size() != 2 {
		t.Fatalf("%s: Size() = %d after re-Register, want 2", name, b.Size())
	}
}

func mkFake(id string) *FakeBackend { return NewFakeBackend(id, 5) }

func TestBalancerContract_RoundRobin(t *testing.T) {
	probeBackendContract(t, "round_robin", func() pkg.Balancer[*FakeBackend] {
		return NewRoundRobinBalancer[*FakeBackend]()
	}, mkFake)
}

func TestBalancerContract_Random(t *testing.T) {
	probeBackendContract(t, "random", func() pkg.Balancer[*FakeBackend] {
		return NewRandomBalancer[*FakeBackend]()
	}, mkFake)
}

func TestBalancerContract_LeastConnections(t *testing.T) {
	probeBackendContract(t, "least_connections", func() pkg.Balancer[*FakeBackend] {
		return NewLeastConnectionsBalancer[*FakeBackend](false)
	}, mkFake)
}

func TestBalancerContract_WeightedRoundRobin(t *testing.T) {
	probeBackendContract(t, "weighted_round_robin", func() pkg.Balancer[*FakeBackend] {
		return NewWeightedRoundRobinBalancer[*FakeBackend]()
	}, mkFake)
}

func TestBalancerContract_WeightedRandom(t *testing.T) {
	probeBackendContract(t, "weighted_random", func() pkg.Balancer[*FakeBackend] {
		return NewWeightedRandomBalancer[*FakeBackend]()
	}, mkFake)
}

func TestBalancerContract_IPHashing(t *testing.T) {
	probeBackendContract(t, "ip_hashing", func() pkg.Balancer[*FakeBackend] {
		return NewIPHashingBalancer[*FakeBackend](4)
	}, mkFake)
}

func TestBalancerContract_Adaptive(t *testing.T) {
	probeBackendContract(t, "adaptive", func() pkg.Balancer[*loadableBackend] {
		return NewAdaptiveBalancer[*loadableBackend]()
	}, func(id string) *loadableBackend {
		return &loadableBackend{FakeBackend: NewFakeBackend(id, 5)}
	})
}

// TestIPHashingBalancerReplicaClamp: replicas below three would make the
// ring degenerate (too few virtual nodes to spread keys), so the
// constructor must lift it to the floor.
func TestIPHashingBalancerReplicaClamp(t *testing.T) {
	b := NewIPHashingBalancer[*FakeBackend](1)
	if err := b.Register(mkFake("only")); err != nil {
		t.Fatal(err)
	}
	// three replicas of one backend: every key resolves to it, and the
	// ring actually has three virtual entries backing it up
	for _, key := range []string{"a", "b", "c", "192.168.0.1:1"} {
		next, err := b.Next(key)
		if err != nil {
			t.Fatalf("Next(%q): %v", key, err)
		}
		if next.Id() != "only" {
			t.Fatalf("Next(%q) = %s, want the only backend", key, next.Id())
		}
	}
}

// TestLeastConnectionsBalancerUpdateCountRotates covers the
// updateCount=true mode: every pick bumps the winner's count, which turns
// least-connections into a rotation — ten picks over two idle backends
// must split evenly instead of pinning to the first.
func TestLeastConnectionsBalancerUpdateCountRotates(t *testing.T) {
	b := NewLeastConnectionsBalancer[*FakeBackend](true)
	a, c := NewFakeBackend("a", 5), NewFakeBackend("c", 5)
	if err := b.Register(a); err != nil {
		t.Fatal(err)
	}
	if err := b.Register(c); err != nil {
		t.Fatal(err)
	}

	got := map[string]int{}
	for i := 0; i < 10; i++ {
		next, err := b.Next("")
		if err != nil {
			t.Fatalf("Next(): %v", err)
		}
		got[next.Id()]++
	}
	if got["a"] != 5 || got["c"] != 5 {
		t.Fatalf("picks = %v, want an even 5/5 split", got)
	}
	if a.Count() != 5 || c.Count() != 5 {
		t.Fatalf("counts = a:%d c:%d, want 5 each", a.Count(), c.Count())
	}
}
