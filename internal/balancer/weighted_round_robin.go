package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"sort"
	"sync"
)

type WeightedRoundRobinBalancer struct {
	backends           []pkg.WeightBackend
	indexToTotalWeight []int
	currentWeight      int
	rwMutex            sync.RWMutex
}

var _ pkg.Balancer = (*WeightedRoundRobinBalancer)(nil)

func NewWeightedRoundRobinBalancer() *WeightedRoundRobinBalancer {
	return &WeightedRoundRobinBalancer{
		backends:           make([]pkg.WeightBackend, 0),
		currentWeight:      0,
		indexToTotalWeight: make([]int, 0),
		rwMutex:            sync.RWMutex{},
	}
}

func (b *WeightedRoundRobinBalancer) Next(key string) (pkg.Backend, error) {
	b.rwMutex.RLock()
	defer b.rwMutex.RUnlock()
	size := len(b.backends)
	if size == 0 {
		return nil, errors.New("no backends registered")
	}
	if size == 1 {
		return b.backends[0], nil
	}

	end := b.indexToTotalWeight[size-1]
	start := b.indexToTotalWeight[0]
	b.currentWeight = (b.currentWeight+1)%(end-start) + start
	index := sort.Search(size, func(i int) bool {
		return b.indexToTotalWeight[i] >= b.currentWeight
	})
	if index >= size {
		index = 0
	}
	return b.backends[index], nil
}

func (b *WeightedRoundRobinBalancer) buildIndexToTotalWeight() {
	if len(b.backends) == 0 {
		return
	}
	b.indexToTotalWeight = make([]int, len(b.backends))
	totalWeight := 0
	for i := 0; i < len(b.backends); i++ {
		totalWeight += b.indexToTotalWeight[i]
		b.indexToTotalWeight[i] = totalWeight
	}
}

func (b *WeightedRoundRobinBalancer) Register(backend pkg.Backend) error {
	weightedBackend, ok := backend.(pkg.WeightBackend)
	if !ok {
		return errors.New("backend is not WeightBackend")
	}
	if weightedBackend.Weight() <= 0 {
		return errors.New("backend has invalid Weight")
	}
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for _, back := range b.backends {
		if back.Id() != weightedBackend.Id() {
			continue
		}
		if back.Weight() != weightedBackend.Weight() {
			back.SetWeight(weightedBackend.Weight())
			b.buildIndexToTotalWeight()
		}
		return nil
	}
	b.backends = append(b.backends, weightedBackend)
	b.buildIndexToTotalWeight()
	return nil
}

func (b *WeightedRoundRobinBalancer) Unregister(unregisterBackend pkg.Backend) error {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for i, back := range b.backends {
		if back.Id() == unregisterBackend.Id() {
			b.backends = append(b.backends[:i], b.backends[i+1:]...)
			b.buildIndexToTotalWeight()
			return nil
		}
	}
	return nil
}

func (b *WeightedRoundRobinBalancer) Size() int {
	b.rwMutex.RLock()
	defer b.rwMutex.RUnlock()
	return len(b.backends)
}

func (b *WeightedRoundRobinBalancer) Iterate(f func(pkg.Backend) bool) {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for _, backend := range b.backends {
		if !f(backend) {
			break
		}
	}
}
