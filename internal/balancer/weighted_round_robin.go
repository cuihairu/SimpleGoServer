package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"sync"
)

type WeightedRoundRobinBalancer struct {
	backends      []pkg.WeightBackend
	currentWeight int
	totalWeight   int
	rwMutex       sync.RWMutex
}

var _ pkg.Balancer = (*WeightedRoundRobinBalancer)(nil)

func NewWeightedRoundRobinBalancer() *WeightedRoundRobinBalancer {
	return &WeightedRoundRobinBalancer{
		backends:      make([]pkg.WeightBackend, 0),
		currentWeight: 0,
		totalWeight:   0,
		rwMutex:       sync.RWMutex{},
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
			b.totalWeight -= back.Weight() - weightedBackend.Weight()
			back.SetWeight(weightedBackend.Weight())
		}
		return nil
	}
	b.backends = append(b.backends, weightedBackend)
	b.totalWeight += weightedBackend.Weight()
	return nil
}

func (b *WeightedRoundRobinBalancer) Unregister(unregisterBackend pkg.Backend) error {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for i, back := range b.backends {
		if back.Id() == unregisterBackend.Id() {
			b.backends = append(b.backends[:i], b.backends[i+1:]...)
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
