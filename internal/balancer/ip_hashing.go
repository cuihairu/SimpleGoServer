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
	rwMutex        sync.RWMutex
}

func NewIPHashingBalancer[T pkg.Backend](replicas int) *IPHashingBalancer[T] {
	if replicas < 3 {
		replicas = 3
	}
	return &IPHashingBalancer[T]{
		circle:   make(map[uint32]T),
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
	hash, err := hashKey(key)
	if err != nil {
		return backend, err
	}
	index := sort.Search(len(I.sortedHashList), func(i int) bool {
		return I.sortedHashList[i] >= hash
	})
	if index == len(I.sortedHashList) {
		index = 0
	}
	return I.circle[I.sortedHashList[index]], nil
}

func hashKey(key string) (uint32, error) {
	hasher := fnv.New32a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return 0, err
	}
	return hasher.Sum32(), nil
}

func (I *IPHashingBalancer[T]) Register(backend T) error {
	I.rwMutex.Lock()
	defer I.rwMutex.Unlock()
	for i := 0; i < I.replicas; i++ {
		replicaKey := backend.Id() + strconv.Itoa(i)
		hash, err := hashKey(replicaKey)
		if err != nil {
			return err
		}
		I.circle[hash] = backend
		I.sortedHashList = append(I.sortedHashList, hash)
	}
	sort.Slice(I.sortedHashList, func(i, j int) bool {
		return I.sortedHashList[i] < I.sortedHashList[j]
	})
	I.size++
	return nil
}

func (I *IPHashingBalancer[T]) Unregister(backend T) error {
	I.rwMutex.Lock()
	defer I.rwMutex.Unlock()
	for i := 0; i < I.replicas; i++ {
		replicaKey := backend.Id() + strconv.Itoa(i)
		hash, err := hashKey(replicaKey)
		if err != nil {
			return err
		}
		delete(I.circle, hash)
		index := sort.Search(len(I.sortedHashList), func(i int) bool {
			return I.sortedHashList[i] >= hash
		})
		if index < len(I.sortedHashList) && I.sortedHashList[index] == hash {
			I.sortedHashList = append(I.sortedHashList[:index], I.sortedHashList[index+1:]...)
		}
	}
	I.size--
	return nil
}

func (I *IPHashingBalancer[T]) Size() int {
	I.rwMutex.RLock()
	defer I.rwMutex.RUnlock()
	return I.size
}

func (I *IPHashingBalancer[T]) Iterate(f func(b T) bool) {
	I.rwMutex.Lock()
	defer I.rwMutex.Unlock()
	// the ring holds one entry per virtual node; callers of Iterate expect
	// one visit per backend (reactor Start/Stop route every worker through
	// it), so dedupe by id
	seen := make(map[string]struct{}, I.size)
	for _, hash := range I.sortedHashList {
		backend := I.circle[hash]
		id := backend.Id()
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if !f(backend) {
			break
		}
	}
}

var _ pkg.Balancer[pkg.Backend] = (*IPHashingBalancer[pkg.Backend])(nil)
