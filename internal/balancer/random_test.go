package balancer

import (
	"strconv"
	"testing"
)

func TestRandomBalancer_Next(t *testing.T) {
	balancer := NewRandomBalancer[*FakeBackend]()
	bulk := NewFakeBackendBulk(10)
	for i, backend := range bulk {
		err := balancer.Register(backend)
		if err != nil {
			t.Error(err)
		}
		next, err := balancer.Next(strconv.Itoa(i))
		if err != nil {
			t.Error(err)
		}
		t.Logf("select backend id :%s", next.Id())
	}
}
