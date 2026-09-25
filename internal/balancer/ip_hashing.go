package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type IPHashingBalancer[T pkg.Backend] struct {
	circle         map[uint32]T
	replicas       int
	size           int
	sortedHashList []uint32
	// id → backend: the ring only stores virtual nodes, so membership is
	// tracked separately — Register dedupes on it and Unregister stays
	// idempotent instead of driving size negative on unknown backends
	members map[string]T
	rwMutex sync.RWMutex
}

func NewIPHashingBalancer[T pkg.Backend](replicas int) *IPHashingBalancer[T] {
	if replicas < 3 {
		replicas = 3
	}
	return &IPHashingBalancer[T]{
		circle:   make(map[uint32]T),
		members:  make(map[string]T),
		replicas: replicas,
		rwMutex:  sync.RWMutex{},
	}
}

func (I *IPHashingBalancer[T]) Next(key string) (T, error) {
	I.rwMutex.RLock()
	defer I.rwMutex.RUnlock()
	var backend T
	if len(I.circle) == 0 {
		return backend, errors.New("no replicas")
	}
	hash := hashKey(key)
	index := sort.Search(len(I.sortedHashList), func(i int) bool {
		return I.sortedHashList[i] >= hash
	})
	if index == len(I.sortedHashList) {
		index = 0
	}
	return I.circle[I.sortedHashList[index]], nil
}

// hashKey is fnv-1a over the key. fnv's Write is a pure computation that
// never fails, so there is deliberately no error to propagate.
func hashKey(key string) uint32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key)) // fnv's Write never returns an error
	return hasher.Sum32()
}

func (I *IPHashingBalancer[T]) Register(backend T) error {
	I.rwMutex.Lock()
	defer I.rwMutex.Unlock()
	if _, ok := I.members[backend.Id()]; ok {
		return nil
	}
	for i := 0; i < I.replicas; i++ {
		replicaKey := backend.Id() + strconv.Itoa(i)
		hash := hashKey(replicaKey)
		I.circle[hash] = backend
		I.sortedHashList = append(I.sortedHashList, hash)
	}
	sort.Slice(I.sortedHashList, func(i, j int) bool {
		return I.sortedHashList[i] < I.sortedHashList[j]
	})
	I.members[backend.Id()] = backend
	I.size++
	return nil
}

func (I *IPHashingBalancer[T]) Unregister(backend T) error {
	I.rwMutex.Lock()
	defer I.rwMutex.Unlock()
	// idempotent: removing an unknown backend must not touch the ring or
	// the size counter
	if _, ok := I.members[backend.Id()]; !ok {
		return nil
	}
	for i := 0; i < I.replicas; i++ {
		replicaKey := backend.Id() + strconv.Itoa(i)
		hash := hashKey(replicaKey)
		delete(I.circle, hash)
		index := sort.Search(len(I.sortedHashList), func(i int) bool {
			return I.sortedHashList[i] >= hash
		})
		if index < len(I.sortedHashList) && I.sortedHashList[index] == hash {
			I.sortedHashList = append(I.sortedHashList[:index], I.sortedHashList[index+1:]...)
		}
	}
	delete(I.members, backend.Id())
	I.size--
	return nil
}

func (I *IPHashingBalancer[T]) Size() int {
	I.rwMutex.RLock()
	defer I.rwMutex.RUnlock()
	return I.size
}

func (I *IPHashingBalancer[T]) Iterate(f func(b T) bool) {
	I.rwMutex.RLock()
	defer I.rwMutex.RUnlock()
	// members holds one entry per backend, so each is visited exactly once
	for _, backend := range I.members {
		if !f(backend) {
			break
		}
	}
}

var _ pkg.Balancer[pkg.Backend] = (*IPHashingBalancer[pkg.Backend])(nil)
