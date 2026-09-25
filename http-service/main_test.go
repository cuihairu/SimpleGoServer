package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
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

// main only translates run's code into os.Exit; the stub proves the
// translation without terminating the test binary (same shape as
// cmd/cli).
func TestMainTranslatesExitCode(t *testing.T) {
	origArgs, origExit := os.Args, osExit
	defer func() { os.Args, osExit = origArgs, origExit }()

	code := -1
	osExit = func(c int) { code = c }
	os.Args = []string{"http-service", "-nope"}
	main()
	if code != 2 {
		t.Fatalf("main() exit code = %d, want 2 for a bad flag", code)
	}
}

// Binding a port that is already taken must surface as exit code 1,
// not a silent hang or an exit 0 — this is the errCh branch of run's
// select, the "listen failed" face of the lifecycle.
func TestRunBindFailureReturnsOne(t *testing.T) {
	port := freePort(t)
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	defer l.Close()
	if code := run([]string{"-addr", fmt.Sprintf("127.0.0.1:%d", port)}); code != 1 {
		t.Fatalf("run on an occupied port: %d, want 1", code)
	}
}

// A request whose body never finishes parks its handler in ReadAll:
// shutdown must give up after the timeout and run must report 1 — that
// deadline is the difference between a slow client and an unkillable
// process.
func TestRunShutdownTimeoutReturnsOne(t *testing.T) {
	defer goleak.VerifyNone(t)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	codeCh := make(chan int, 1)
	go func() { codeCh <- run([]string{"-addr", addr, "-shutdown-timeout", "200ms"}) }()

	base := fmt.Sprintf("http://%s", addr)
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(base + "/healthz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never answered /healthz")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Open a raw connection that promises 64 bytes but delivers five.
	// Expect: 100-continue makes handler readiness observable: net/http
	// writes the interim 100 only once the handler's first body Read
	// happens — i.e. it is already parked in ReadAll. Waiting for that
	// removes the race where SIGTERM lands before the request is even
	// accepted, which would make Shutdown return nil instantly.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "PUT /kv/stuck HTTP/1.1\r\nHost: x\r\nContent-Length: 64\r\nExpect: 100-continue\r\n\r\nshort")

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 256)
	var seen []byte
	for {
		n, err := conn.Read(buf)
		seen = append(seen, buf[:n]...)
		if err != nil {
			t.Fatalf("waiting for 100 Continue: %v (got %q)", err, seen)
		}
		if strings.Contains(string(seen), "100 Continue") {
			break
		}
	}

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	select {
	case code := <-codeCh:
		if code != 1 {
			t.Fatalf("run with a stuck handler = %d, want 1 (shutdown timeout)", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after the shutdown timeout")
	}

	// drain until the server side gives up on this connection, so the
	// parked handler has exited before goleak looks around
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = io.ReadAll(io.LimitReader(conn, 1<<16))
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
