package executor

import (
	"context"
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg"
	"sync"
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

// TestAsyncExecutorStopDoesNotPanicWorkers pins the shutdown contract:
// Stop used to close the task channels, which made any in-flight send
// panic ("send on closed channel") and handed idle workers zero values
// to call — both process-killing. Concurrent submit/stop must be safe.
func TestAsyncExecutorStopDoesNotPanicWorkers(t *testing.T) {
	executor := NewAsyncExecutor[int](4)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				executor.Execute(func(context.Context) {})
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	executor.Stop()
	wg.Wait()
	_ = executor.Submit(func(context.Context) (int, error) { return 0, nil })
}

// TestAsyncExecutorSubmitAfterStopReturnsCancelledFuture: Get on such a
// future must not block forever — it resolves immediately as cancelled.
func TestAsyncExecutorSubmitAfterStopReturnsCancelledFuture(t *testing.T) {
	executor := NewAsyncExecutor[int](2)
	executor.Stop()

	future := executor.Submit(func(context.Context) (int, error) { return 42, nil })
	deadline := time.Now().Add(time.Second)
	for !future.IsDone() {
		if time.Now().After(deadline) {
			t.Fatal("future from a stopped executor never resolved")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if future.IsCancelled() != true {
		t.Fatal("future from a stopped executor should report cancelled")
	}
	if v, err := future.Get(); err == nil || v != 0 {
		t.Fatalf("Get() = (%d, %v), want (0, cancelled)", v, err)
	}
}
