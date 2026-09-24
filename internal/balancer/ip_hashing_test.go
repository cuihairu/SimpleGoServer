package balancer

import (
	"fmt"
	"math/rand"
	"testing"
)

type FakeBackend struct {
	id     string
	weight int
	count  int
}

func (w *FakeBackend) Count() int {
	return w.count
}

func (w *FakeBackend) SetCount(count int) {
	w.count = count
}

func (w *FakeBackend) Id() string {
	return w.id
}

func (w *FakeBackend) Weight() int {
	return w.weight
}

func (w *FakeBackend) SetWeight(weight int) {
	w.weight = weight
}

func NewFakeBackend(id string, weight int) *FakeBackend {
	return &FakeBackend{id: id, weight: weight}
}

func NewFakeBackendBulk(batch int) []*FakeBackend {
	l := make([]*FakeBackend, batch)
	for i := range l {
		l[i] = NewFakeBackend(fmt.Sprintf("fake_backend_%d", i), batch)
	}
	return l
}

func TestIPHashingBalancer_Next(t *testing.T) {
	balancer := NewIPHashingBalancer[*FakeBackend](6)
	for i := 0; i < 10; i++ {
		w := NewFakeBackend(fmt.Sprintf("%d", i), i)
		err := balancer.Register(w)
		if err != nil {
			t.Fatalf("Error registering worker: %s", err)
		}
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%d", rand.Intn(10000))
		next, err := balancer.Next(key)
		if err != nil {
			t.Fatalf("error getting next key: %v", err)
		}
		t.Logf("select id :%s\n", next.Id())
	}
	ip := "192.168.1.1:50002"
	next, err := balancer.Next(ip)
	if err != nil {
		t.Fatalf("error getting next key: %v", err)
	}
	next2, err := balancer.Next(ip)
	if err != nil {
		t.Fatalf("error getting next key: %v", err)
	}
	if next != next2 {
		t.Fatalf("next and next2 not equal")
	}
}

// TestIPHashingBalancer_IterateVisitsEachBackendOnce pins the Iterate
// contract: one visit per backend, not per virtual node — reactor's
// Start/Stop hand every worker to Iterate, and per-replica visits would
// start each worker several times over.
func TestIPHashingBalancer_IterateVisitsEachBackendOnce(t *testing.T) {
	a := NewFakeBackend("a", 1)
	b := NewFakeBackend("b", 1)
	balancer := NewIPHashingBalancer[*FakeBackend](4)
	for _, backend := range []*FakeBackend{a, b} {
		if err := balancer.Register(backend); err != nil {
			t.Fatal(err)
		}
	}
	visited := map[string]int{}
	balancer.Iterate(func(backend *FakeBackend) bool {
		visited[backend.Id()]++
		return true
	})
	if len(visited) != 2 || visited["a"] != 1 || visited["b"] != 1 {
		t.Fatalf("Iterate visited %v, want exactly one visit per backend", visited)
	}
}
