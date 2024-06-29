package executor

import (
	"context"
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg"
	"testing"
	"time"
)

func TestNewAsyncExecutor(t *testing.T) {
	var executor pkg.Executor[int] = NewAsyncExecutor[int](2)
	future := executor.Submit(func(cxt context.Context) (int, error) {
		fmt.Printf("start: %+v\n", time.Now())
		time.Sleep(2 * time.Second)
		return 1, nil
	})
	timeout, err := future.GetWithTimeout(30 * time.Second)
	if err != nil {
		t.Logf("err: %+v\n", err)
	} else {
		t.Logf("result: %+v\n", timeout)
	}
	executor.Shutdown()
}
