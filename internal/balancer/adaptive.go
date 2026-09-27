package balancer

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/cuihairu/simplegoserver/pkg"
)

// LoadAware is the contract an adaptive balancer needs from a backend:
// a live load figure that the backend itself keeps up to date. Anything
// monotonic in "how busy am I" works — active connection count, in-flight
// requests, EWMA latency — the balancer only compares values.
type LoadAware interface {
	pkg.Backend
	Load() float64
}

// AdaptiveBalancer picks the backend with the lowest live load, which makes
// it self-correcting: no static weights to tune, and a backend that falls
// behind stops receiving connections until it catches up. Ties are broken
// randomly so equally idle backends share traffic evenly instead of
// pinning to the first one.
type AdaptiveBalancer[T LoadAware] struct {
	backends []T
	rwMutex  sync.RWMutex
	random   *rand.Rand
}

var _ pkg.Balancer[LoadAware] = (*AdaptiveBalancer[LoadAware])(nil)

func NewAdaptiveBalancer[T LoadAware]() *AdaptiveBalancer[T] {
	return &AdaptiveBalancer[T]{
		backends: make([]T, 0),
		rwMutex:  sync.RWMutex{},
		random:   rand.New(rand.NewSource(time.Now().UnixNano())), // #nosec G404 -- tie-break randomness only, no security role
	}
}

func (a *AdaptiveBalancer[T]) Next(key string) (T, error) {
	a.rwMutex.Lock()
	defer a.rwMutex.Unlock()
	var next T
	size := len(a.backends)
	if size == 0 {
		return next, errors.New("no backends registered")
	}
	if size == 1 {
		return a.backends[0], nil
	}
	// linear scan: comparing a handful of workers is cheaper than
	// maintaining a heap for a small, mostly-static backend set.
	//
	// Each backend's Load() is read exactly once and the minimum and the tie
	// set are derived from that single pass. Load() is a live figure — other
	// goroutines add and remove connections while this runs — so reading it
	// twice (minimum in one loop, then ties against freshly read values) could
	// leave nothing equal to the minimum that had already gone stale; the tie
	// loop then assigned nothing and the caller got the zero value of T with a
	// nil error, i.e. a nil *Worker, which panicked one frame later in
	// AddConn. Seeding next with backends[0] keeps the function total: the
	// result is always a registered backend, and only ever one whose own
	// observation equalled the best seen so far.
	best := a.backends[0].Load()
	next = a.backends[0]
	// reservoir-style pick among the tied winners so equally idle backends
	// share traffic evenly instead of pinning to the first one
	ties := 1
	for i := 1; i < size; i++ {
		switch load := a.backends[i].Load(); {
		case load < best:
			best, next, ties = load, a.backends[i], 1
		case load == best:
			ties++
			if a.random.Intn(ties) == 0 {
				next = a.backends[i]
			}
		}
	}
	return next, nil
}

func (a *AdaptiveBalancer[T]) Register(backend T) error {
	a.rwMutex.Lock()
	defer a.rwMutex.Unlock()
	for _, back := range a.backends {
		if back.Id() == backend.Id() {
			return nil
		}
	}
	a.backends = append(a.backends, backend)
	return nil
}

func (a *AdaptiveBalancer[T]) Unregister(backend T) error {
	a.rwMutex.Lock()
	defer a.rwMutex.Unlock()
	for i, back := range a.backends {
		if back.Id() == backend.Id() {
			a.backends = append(a.backends[:i], a.backends[i+1:]...)
			return nil
		}
	}
	return nil
}

func (a *AdaptiveBalancer[T]) Size() int {
	a.rwMutex.RLock()
	defer a.rwMutex.RUnlock()
	return len(a.backends)
}

func (a *AdaptiveBalancer[T]) Iterate(f func(b T) bool) {
	a.rwMutex.RLock()
	defer a.rwMutex.RUnlock()
	for _, back := range a.backends {
		if !f(back) {
			break
		}
	}
}
