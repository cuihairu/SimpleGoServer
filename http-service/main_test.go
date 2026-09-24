package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// freePort reserves an ephemeral port and hands it back: the listener
// is closed immediately, so there is a tiny reserve-and-race window,
// but no portable way to pass an open listener through run's flag API.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestRunBadFlags(t *testing.T) {
	if code := run([]string{"-nope"}); code != 2 {
		t.Fatalf("run with unknown flag: %d, want 2", code)
	}
}

// The full lifecycle under go test: bind, serve, answer real requests,
// trap a genuine SIGTERM, drain, exit 0, refuse new connections.
func TestRunEndToEndAndGracefulShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)

	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()

	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	codeCh := make(chan int, 1)
	go func() { codeCh <- run([]string{"-addr", fmt.Sprintf("127.0.0.1:%d", port)}) }()

	// Poll until the server answers. By the time it does, the signal
	// handler above is guaranteed to be installed (NotifyContext runs
	// before ListenAndServe), so the SIGTERM below cannot kill us.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("server never answered /healthz")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// One write and one read through the real stack.
	req, err := http.NewRequest(http.MethodPut, base+"/kv/k", strings.NewReader(`{"ok":true}`))
	if err != nil {
		t.Fatalf("build PUT: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT: resp=%v err=%v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	resp, err = client.Get(base + "/kv/k")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: resp=%v err=%v", resp, err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil || got["ok"] != true {
		t.Fatalf("GET body: %s err=%v", b, err)
	}

	// Exactly one SIGTERM: the NotifyContext inside run traps it.
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	select {
	case code := <-codeCh:
		if code != 0 {
			t.Fatalf("run exited with %d, want 0", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after SIGTERM")
	}

	// After Shutdown the listener is closed: new connections must be
	// refused, not queued for a server that no longer exists.
	if _, err := client.Get(base + "/healthz"); err == nil {
		t.Fatal("connection accepted after shutdown")
	}
}
