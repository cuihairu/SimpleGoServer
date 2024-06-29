package executor

import (
	"context"
	"testing"
	"time"
)

func TestNewFuture(t *testing.T) {
	f := NewFuture[string](context.Background(), func(ctx context.Context) (string, error) {
		time.Sleep(3 * time.Second)
		return "------", nil
	})
	go f.Do()
	ret, err := f.GetWithTimeout(3 * time.Second)
	if err != nil {
		t.Logf("err: %+v\n", err)
	} else {
		t.Logf("result: %+v\n", ret)
	}
}
