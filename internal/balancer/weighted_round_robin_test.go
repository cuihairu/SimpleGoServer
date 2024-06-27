package balancer

import (
	"fmt"
	"testing"
)

func TestWeightedRoundRobinBalancer_Next(t *testing.T) {
	balancer := NewWeightedRoundRobinBalancer()
	bulk := NewFakeBackendBulk(10)
	for _, backend := range bulk {
		backend.SetWeight(1)
		err := balancer.Register(backend)
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 100; i++ {
		next, err := balancer.Next(fmt.Sprintf("%d", i))
		if err != nil {
			t.Error(err)
		}
		t.Logf("select backend %s", next.Id())
	}

}
