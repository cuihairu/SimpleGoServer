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

// TestWeightedRandomBalancer_RegisterWeightTable pins the weight rules in a
// table: positive weights register, non-positive weights are rejected. The
// zero+positive and positive+negative mixes that used to corrupt preSum
// (non-monotonic, breaking sort.Search) are exactly the non-positive cases
// this table covers — they can no longer reach the distribution at all.
func TestWeightedRandomBalancer_RegisterWeightTable(t *testing.T) {
	cases := []struct {
		name   string
		weight int
		ok     bool
	}{
		{"positive", 1, true},
		{"zero", 0, false},
		{"negative", -1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			balancer := NewWeightedRandomBalancer[*FakeBackend]()
			err := balancer.Register(NewFakeBackend("b", tc.weight))
			if tc.ok && err != nil {
				t.Fatalf("Register(weight=%d) = %v, want nil", tc.weight, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("Register(weight=%d) = nil, want an error", tc.weight)
			}
			wantSize := 0
			if tc.ok {
				wantSize = 1
			}
			if balancer.Size() != wantSize {
				t.Fatalf("Size() = %d, want %d", balancer.Size(), wantSize)
			}
		})
	}
}

// TestWeightedRandomBalancer_NextRefusesMutatedNonPositiveWeight pins the
// degenerate case where a registered backend's weight is mutated to zero or
// negative after registration (bypassing the Register check): preSum[size]
// becomes non-positive, and Next must refuse instead of calling rand.Intn(0)
// (which would panic) or silently serving an arbitrary backend.
func TestWeightedRandomBalancer_NextRefusesMutatedNonPositiveWeight(t *testing.T) {
	a := NewFakeBackend("a", 5)
	b := NewFakeBackend("b", 5)
	balancer := NewWeightedRandomBalancer[*FakeBackend]()
	for _, backend := range []*FakeBackend{a, b} {
		if err := balancer.Register(backend); err != nil {
			t.Fatal(err)
		}
	}
	// mutate after the fact, bypassing the Register check, then force a
	// preSum rebuild (as a real Register/Unregister would): the total must go
	// non-positive for the safety net to refuse.
	a.SetWeight(0)
	b.SetWeight(0)
	balancer.buildPreSum()
	if _, err := balancer.Next("k"); err == nil {
		t.Fatal("Next on a mutated non-positive total weight must fail, not panic or serve an arbitrary backend")
	}
}

// A one-backend shortcut would bypass all preSum validation and hide a
// post-registration mutation from callers.
func TestWeightedRandomBalancer_NextRefusesMutatedSingleNonPositiveWeight(t *testing.T) {
	backend := NewFakeBackend("only", 5)
	balancer := NewWeightedRandomBalancer[*FakeBackend]()
	if err := balancer.Register(backend); err != nil {
		t.Fatal(err)
	}
	backend.SetWeight(0)
	balancer.buildPreSum()
	if _, err := balancer.Next("k"); err == nil {
		t.Fatal("single-backend Next on a mutated non-positive weight must fail")
	}
}

// TestWeightedRandomBalancer_DistributionMatchesWeights verifies the
// selection frequency follows the weights on a positive-only table: a 1:3
// split must hit the heavier backend far more often, and both must be
// reachable. A non-monotonic preSum would skew this hard (the heavier
// backend could go unselected entirely).
func TestWeightedRandomBalancer_DistributionMatchesWeights(t *testing.T) {
	a := NewFakeBackend("a", 1)
	b := NewFakeBackend("b", 3)
	balancer := NewWeightedRandomBalancer[*FakeBackend]()
	for _, backend := range []*FakeBackend{a, b} {
		if err := balancer.Register(backend); err != nil {
			t.Fatal(err)
		}
	}
	hits := map[string]int{}
	const draws = 10000
	for i := 0; i < draws; i++ {
		next, err := balancer.Next("k")
		if err != nil {
			t.Fatal(err)
		}
		hits[next.Id()]++
	}
	// a gets ~25%, b ~75%; generous bounds keep this deterministic even on a
	// busy scheduler (the only randomness is math/rand's distribution, which
	// is stable far tighter than these bounds).
	if hits["a"] < draws/5 || hits["a"] > draws*4/10 {
		t.Fatalf("hits[a] = %d over %d draws, want roughly 25%%", hits["a"], draws)
	}
	if hits["b"] < draws*6/10 || hits["b"] > draws*9/10 {
		t.Fatalf("hits[b] = %d over %d draws, want roughly 75%%", hits["b"], draws)
	}
}

// TestWeightedRandomBalancer_NextRefusesMutatedNegativeWeightWithPositiveSum
// pins the safety net that a positive total alone cannot provide: a backend
// mutated to a negative weight after registration (bypassing the Register
// check) can leave the sum positive while making preSum non-monotonic. Next
// must refuse rather than run a skewed distribution that can silently omit
// a positive-weight backend.
func TestWeightedRandomBalancer_NextRefusesMutatedNegativeWeightWithPositiveSum(t *testing.T) {
	a := NewFakeBackend("a", 5)
	b := NewFakeBackend("b", 5)
	balancer := NewWeightedRandomBalancer[*FakeBackend]()
	for _, backend := range []*FakeBackend{a, b} {
		if err := balancer.Register(backend); err != nil {
			t.Fatal(err)
		}
	}

	// Mutate after the fact, bypassing the Register check, then force a
	// preSum rebuild as a real Register/Unregister would. The sum stays
	// positive, but preSum becomes non-monotonic: [0,5,4].
	a.SetWeight(5)
	b.SetWeight(-1)
	balancer.buildPreSum()
	if _, err := balancer.Next("k"); err == nil {
		t.Fatal("Next on a mutated negative weight with a positive total must fail, not skew the distribution")
	}
}
