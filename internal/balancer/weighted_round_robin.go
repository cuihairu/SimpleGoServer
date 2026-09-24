package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"sync"
)

type WeightedRoundRobinBalancer[T pkg.WeightBackend] struct {
	backends      []T
	currentWeight int
	totalWeight   int
	rwMutex       sync.RWMutex
}

var _ pkg.Balancer[pkg.WeightBackend] = (*WeightedRoundRobinBalancer[pkg.WeightBackend])(nil)

func NewWeightedRoundRobinBalancer[T pkg.WeightBackend]() *WeightedRoundRobinBalancer[T] {
	return &WeightedRoundRobinBalancer[T]{
		backends:      make([]T, 0),
		currentWeight: 0,
		totalWeight:   0,
		rwMutex:       sync.RWMutex{},
	}
}

func (b *WeightedRoundRobinBalancer[T]) Next(key string) (T, error) {
	// a write lock, not RLock: Next advances b.currentWeight
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	size := len(b.backends)
	var back T
	if size == 0 {
		return back, errors.New("no backends registered")
	}
	if size == 1 {
		return b.backends[0], nil
	}
	b.currentWeight = (b.currentWeight + 1) % b.totalWeight
	current := b.currentWeight
	for i, backend := range b.backends {
		if current < backend.Weight() {
			return b.backends[i], nil
		}
		current -= backend.Weight()
	}
	return b.backends[0], nil
}

func (b *WeightedRoundRobinBalancer[T]) Register(weightedBackend T) error {
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
			b.totalWeight -= back.Weight() - weightedBackend.Weight()
			back.SetWeight(weightedBackend.Weight())
		}
		return nil
	}
	b.backends = append(b.backends, weightedBackend)
	b.totalWeight += weightedBackend.Weight()
	return nil
}

func (b *WeightedRoundRobinBalancer[T]) Unregister(unregisterBackend T) error {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for i, back := range b.backends {
		if back.Id() == unregisterBackend.Id() {
			b.backends = append(b.backends[:i], b.backends[i+1:]...)
			// keep totalWeight honest or the modulo spread grows and the
			// distribution silently skews towards the fallback branch
			b.totalWeight -= back.Weight()
			return nil
		}
	}
	return nil
}

func (b *WeightedRoundRobinBalancer[T]) Size() int {
	b.rwMutex.RLock()
	defer b.rwMutex.RUnlock()
	return len(b.backends)
}

func (b *WeightedRoundRobinBalancer[T]) Iterate(f func(T) bool) {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for _, backend := range b.backends {
		if !f(backend) {
			break
		}
	}
}
