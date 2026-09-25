package balancer

import (
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg"
	"math/rand"
	"sort"
	"sync"
)

type WeightedRandomBalancer[T pkg.WeightBackend] struct {
	backends []T
	preSum   []int
	rwMutex  sync.RWMutex
}

func NewWeightedRandomBalancer[T pkg.WeightBackend]() *WeightedRandomBalancer[T] {
	return &WeightedRandomBalancer[T]{
		backends: make([]T, 0),
		rwMutex:  sync.RWMutex{},
	}
}

func (w *WeightedRandomBalancer[T]) Next(key string) (T, error) {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()
	size := len(w.backends)
	var b T
	if size == 0 {
		return b, fmt.Errorf("no backends")
	}
	if size == 1 {
		return w.backends[0], nil
	}
	r := rand.Intn(w.preSum[size]) // #nosec G404 -- distribution fairness only, no security role
	// Search always lands in [1, size]: preSum[size] > r rules out a miss
	// (so index <= size) and preSum[0] == 0 > r would need r < 0 (so
	// index >= 1) — index-1 is always a valid backend slot
	index := sort.Search(len(w.preSum), func(i int) bool {
		return w.preSum[i] > r
	})
	return w.backends[index-1], nil
}

func (w *WeightedRandomBalancer[T]) buildPreSum() {
	w.preSum = make([]int, len(w.backends)+1)
	w.preSum[0] = 0
	for i := 1; i <= len(w.backends); i++ {
		w.preSum[i] = w.preSum[i-1] + w.backends[i-1].Weight()
	}
}

func (w *WeightedRandomBalancer[T]) Register(WeightedBackend T) error {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	for _, back := range w.backends {
		if back.Id() == WeightedBackend.Id() {
			return nil
		}
	}
	w.backends = append(w.backends, WeightedBackend)
	w.buildPreSum()
	return nil
}

func (w *WeightedRandomBalancer[T]) Unregister(b T) error {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	// remove the matching entry itself — the old code dropped the first
	// element no matter which backend matched, silently shifting every
	// preSum index onto the wrong backend
	for i, back := range w.backends {
		if back.Id() == b.Id() {
			w.backends = append(w.backends[:i], w.backends[i+1:]...)
			break
		}
	}
	w.buildPreSum()
	return nil
}

func (w *WeightedRandomBalancer[T]) Size() int {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()
	return len(w.backends)
}

func (w *WeightedRandomBalancer[T]) Iterate(f func(b T) bool) {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	for _, WeightedBackend := range w.backends {
		if !f(WeightedBackend) {
			break
		}
	}
}

var _ pkg.Balancer[pkg.WeightBackend] = (*WeightedRandomBalancer[pkg.WeightBackend])(nil)
