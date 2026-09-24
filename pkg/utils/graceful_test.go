package utils

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"
)

// These tests send real signals to the test process, so they must stay
// serial. keepProcessAlive registers a throwaway handler for the signal
// under test: between `go graceful.Wait()` and Graceful's own
// signal.Notify there is a window where the default disposition — process
// termination — would otherwise win the race and kill the test binary.
func keepProcessAlive(t *testing.T, sig os.Signal) {
	t.Helper()
	ch := make(chan os.Signal, 16)
	signal.Notify(ch, sig)
	t.Cleanup(func() { signal.Stop(ch) })
}

// waitForShutdownSignal resends the signal until Graceful's own Notify has
// landed and the callback fires — a signal sent during the registration
// window only reaches the keeper channel.
func waitForShutdownSignal(t *testing.T, sig syscall.Signal, called <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := syscall.Kill(os.Getpid(), sig); err != nil {
			t.Fatalf("kill: %v", err)
		}
		select {
		case <-called:
			return
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("callback never ran")
		}
	}
}

func TestGracefulShutdownSignal(t *testing.T) {
	var mu sync.Mutex
	var got os.Signal
	called := make(chan struct{})
	graceful := NewGraceful(func(sig os.Signal) {
		mu.Lock()
		got = sig
		mu.Unlock()
		close(called)
	}, nil)
	keepProcessAlive(t, syscall.SIGTERM)

	exited := make(chan struct{})
	go func() {
		graceful.Wait()
		close(exited)
	}()

	waitForShutdownSignal(t, syscall.SIGTERM, called)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the shutdown signal")
	}
	mu.Lock()
	defer mu.Unlock()
	if got != syscall.SIGTERM {
		t.Fatalf("callback signal = %v, want SIGTERM", got)
	}
}

func TestGracefulReloadSignal(t *testing.T) {
	reloaded := make(chan struct{})
	graceful := NewGraceful(func(os.Signal) {}, func() { close(reloaded) })
	keepProcessAlive(t, syscall.SIGHUP)

	exited := make(chan struct{})
	go func() {
		graceful.Wait()
		close(exited)
	}()

	waitForShutdownSignal(t, syscall.SIGHUP, reloaded)
	// Wait must keep running after a reload signal; only Stop ends it
	graceful.Stop()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after Stop")
	}
}

func TestGracefulStop(t *testing.T) {
	graceful := NewGraceful(func(os.Signal) {
		t.Error("shutdown callback must not run on Stop")
	}, nil)
	exited := make(chan struct{})
	go func() {
		graceful.Wait()
		close(exited)
	}()
	graceful.Stop()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after Stop")
	}
}
