package balancer

import (
	"fmt"
	"testing"
)

func TestWeightedRandomBalancer_Next(t *testing.T) {
	bulk := NewFakeBackendBulk(10)
	balancer := NewWeightedRandomBalancer[*FakeBackend]()
	for _, backend := range bulk {
		backend.SetWeight(10)
		err := balancer.Register(backend)
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 100; i++ {
		next, err := balancer.Next(fmt.Sprintf("--------%d", i))
		if err != nil {
			t.Error(err)
		}
		t.Logf("select backend %s", next.Id())
	}
}

// TestWeightedRandomBalancer_UnregisterRemovesMatch pins the identity of the
// removed backend: the old Unregister dropped the first list element no
// matter which backend matched, shifting every preSum index onto the wrong
// backend.
func TestWeightedRandomBalancer_UnregisterRemovesMatch(t *testing.T) {
	a := NewFakeBackend("a", 1)
	b := NewFakeBackend("b", 10)
	c := NewFakeBackend("c", 100)
	balancer := NewWeightedRandomBalancer[*FakeBackend]()
	for _, backend := range []*FakeBackend{a, b, c} {
		if err := balancer.Register(backend); err != nil {
			t.Fatal(err)
		}
	}
	// registering the same id twice must not duplicate it
	if err := balancer.Register(b); err != nil {
		t.Fatal(err)
	}
	if balancer.Size() != 3 {
		t.Fatalf("Size() = %d, want 3 (duplicate Register must be a no-op)", balancer.Size())
	}

	if err := balancer.Unregister(b); err != nil {
		t.Fatal(err)
	}
	if balancer.Size() != 2 {
		t.Fatalf("Size() = %d, want 2", balancer.Size())
	}
	// "b" held 10/111 of the weight, so if it were still in the rotation
	// this many draws would hit it with overwhelming probability
	for i := 0; i < 500; i++ {
		next, err := balancer.Next("k")
		if err != nil {
			t.Fatal(err)
		}
		if next.Id() == "b" {
			t.Fatalf("unregistered backend %q still served after removal", "b")
		}
	}
}
