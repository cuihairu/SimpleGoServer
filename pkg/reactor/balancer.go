package reactor

import (
	"errors"
	"math/rand"
	"net"
)

type Balancer interface {
	Dispatch(ch net.Conn) (*Worker, error)
	Register(wk *Worker) error
	Size() int
	Iterate(f func(wk *Worker) bool)
}

var _ Balancer = (*RandBalancer)(nil)

type RandBalancer struct {
	workers []*Worker
}

func NewRandBalancer() *RandBalancer {
	return &RandBalancer{
		workers: make([]*Worker, 0),
	}
}

func (r *RandBalancer) Dispatch(c net.Conn) (*Worker, error) {
	if len(r.workers) == 0 {
		return nil, errors.New("no workers")
	}
	return r.workers[rand.Intn(len(r.workers))], nil
}

func (r *RandBalancer) Register(wk *Worker) error {
	r.workers = append(r.workers, wk)
	return nil
}

func (r *RandBalancer) Size() int {
	return len(r.workers)
}

func (r *RandBalancer) Iterate(f func(wk *Worker) bool) {
	for _, w := range r.workers {
		if !f(w) {
			break
		}
	}
}

var _ Balancer = (*LeastBalancer)(nil)

type LeastBalancer struct {
	workers []*Worker
}

func NewLeastBalancer() *LeastBalancer {
	return &LeastBalancer{
		workers: make([]*Worker, 0),
	}
}

func (l *LeastBalancer) Dispatch(c net.Conn) (*Worker, error) {
	if len(l.workers) == 0 {
		return nil, errors.New("no workers")
	}
	var leastCountWorker *Worker = nil
	leastCount := 0
	for _, w := range l.workers {
		currentCount := w.Count()
		if currentCount == 0 {
			return w, nil
		}
		if leastCountWorker == nil {
			leastCountWorker = w
			leastCount = currentCount
			continue
		}
		if leastCount < currentCount {
			leastCount = currentCount
			leastCountWorker = w
		}
	}
	return leastCountWorker, nil
}

func (l *LeastBalancer) Register(wk *Worker) error {
	l.workers = append(l.workers, wk)
	return nil
}

func (l *LeastBalancer) Size() int {
	return len(l.workers)
}

func (l *LeastBalancer) Iterate(f func(wk *Worker) bool) {
	for _, w := range l.workers {
		if !f(w) {
			break
		}
	}
}
