package balancer

import (
	"fmt"
	"testing"
)

func TestName(t *testing.T) {
	balancer := NewLeastConnectionsBalancer[*FakeBackend](false)
	bulk := NewFakeBackendBulk(10)
	for _, backend := range bulk {
		err := balancer.Register(backend)
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 200; i++ {
		_, err := balancer.Next(fmt.Sprintf("%d", i))
		if err != nil {
			t.Error(err)
		}
	}
	count := bulk[0].Weight()
	for _, backend := range bulk {
		if backend.Weight() != count {
			t.Errorf("got backend weight %d, want %d", count, backend.Weight())
		}
	}
}
