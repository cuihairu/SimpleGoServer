package balancer

import (
	"fmt"
	"testing"
)

func TestWeightedRandomBalancer_Next(t *testing.T) {
	bulk := NewFakeBackendBulk(10)
	balancer := NewWeightedRandomBalancer[*FakeBackend]()
	for _, backend := range bulk {
		backend.SetWeight(10)
		err := balancer.Register(backend)
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 100; i++ {
		next, err := balancer.Next(fmt.Sprintf("--------%d", i))
		if err != nil {
			t.Error(err)
		}
		t.Logf("select backend %s", next.Id())
	}
}
