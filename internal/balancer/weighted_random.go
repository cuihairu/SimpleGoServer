package balancer

import (
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg"
	"math/rand"
	"sort"
	"sync"
)

type WeightedRandomBalancer struct {
	backends []pkg.WeightBackend
	preSum   []int
	rwMutex  sync.RWMutex
}

func NewWeightedRandomBalancer() *WeightedRandomBalancer {
	return &WeightedRandomBalancer{
		backends: make([]pkg.WeightBackend, 0),
		rwMutex:  sync.RWMutex{},
	}
}

func (w *WeightedRandomBalancer) Next(key string) (pkg.Backend, error) {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()
	size := len(w.backends)
	if size == 0 {
		return nil, fmt.Errorf("no backends")
	}
	if size == 1 {
		return w.backends[0], nil
	}
	r := rand.Intn(w.preSum[size])
	index := sort.Search(len(w.preSum), func(i int) bool {
		return w.preSum[i] > r
	})
	if index > size || index < 0 {
		index = 1
	}
	return w.backends[index-1], nil
}

func (w *WeightedRandomBalancer) buildPreSum() {
	w.preSum = make([]int, len(w.backends)+1)
	w.preSum[0] = 0
	for i := 1; i <= len(w.backends); i++ {
		w.preSum[i] = w.preSum[i-1] + w.backends[i-1].Weight()
	}
}

func (w *WeightedRandomBalancer) Register(b pkg.Backend) error {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	WeightedBackend := b.(pkg.WeightBackend)
	w.backends = append(w.backends, WeightedBackend)
	w.buildPreSum()
	return nil
}

func (w *WeightedRandomBalancer) Unregister(b pkg.Backend) error {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	for _, WeightedBackend := range w.backends {
		if WeightedBackend.Id() == b.Id() {
			w.backends = append(w.backends[:0], w.backends[1:]...)
		}
	}
	w.buildPreSum()
	return nil
}

func (w *WeightedRandomBalancer) Size() int {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()
	return len(w.backends)
}

func (w *WeightedRandomBalancer) Iterate(f func(b pkg.Backend) bool) {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	for _, WeightedBackend := range w.backends {
		if !f(WeightedBackend) {
			break
		}
	}
}

var _ pkg.Balancer = (*WeightedRandomBalancer)(nil)
