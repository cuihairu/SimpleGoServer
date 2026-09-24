package main

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/proto"
	"github.com/spf13/viper"
)

// resetViper isolates the global config state between tests.
func resetViper(t *testing.T) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
}

func TestInitConfigWithExplicitFile(t *testing.T) {
	resetViper(t)
	cfg := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(cfg, []byte("server:\n  port: 1234\n"), 0o600); err != nil {
		t.Fatalf("writing the config: %v", err)
	}
	cfgFile = cfg
	defer func() { cfgFile = "" }()

	initConfig()

	if got := viper.GetInt("server.port"); got != 1234 {
		t.Fatalf("config file values were not loaded: server.port = %d", got)
	}
}

func TestInitConfigWithoutFileKeepsGoing(t *testing.T) {
	resetViper(t)
	// the repo ships no ./config.yml next to the tests, so the search-path
	// read fails and initConfig must carry on with defaults, silently
	cfgFile = ""
	initConfig()
}

// setServerViper pins the whole server config so runServer is deterministic.
func setServerViper(t *testing.T, port int) {
	t.Helper()
	resetViper(t)
	viper.Set("server.network", "tcp")
	viper.Set("server.host", "127.0.0.1")
	viper.Set("server.port", port)
	viper.Set("server.multicore", false)
	viper.Set("server.numWorkers", 1)
	viper.Set("server.lockThread", false)
	viper.Set("server.idleTimeout", 0)
	viper.Set("server.strictHello", false)
}

// reservePort hands back an address the server can bind deterministically.
func reservePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestMainServesUntilTerminated drives the real main(): flags come off
// os.Args, cobra runs rootCmd, runServer serves one real request, and a
// single trapped SIGTERM brings the whole stack down.
func TestMainServesUntilTerminated(t *testing.T) {
	resetViper(t) // earlier tests cleaned up their viper state; rebind flags
	bindFlags()
	addr := reservePort(t)
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"server", "--host", "127.0.0.1", "--port", strconv.Itoa(portOf(t, addr))}

	go func() {
		// Wait() has installed its signal handlers long before this fires;
		// exactly one SIGTERM is sent, and Graceful traps it
		time.Sleep(800 * time.Millisecond)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		main()
	}()

	// wait for the listener to come up, then serve one real request so the
	// pipeline initializer runs end to end
	var client *proto.Client
	deadline := time.Now().Add(3 * time.Second)
	for {
		var err error
		client, err = proto.Dial(addr, func(*proto.Frame) {})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never came up: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := client.Call("echo", "hi", 2*time.Second); err != nil {
		t.Fatalf("Call(echo): %v", err)
	}
	_ = client.CloseGracefully(time.Second)

	<-done // main returned once SIGTERM triggered the graceful shutdown
}

// portOf extracts the numeric port from a host:port address.
func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parsing port %q: %v", portStr, err)
	}
	return port
}

func TestRunServerPanicsOnUnusableListener(t *testing.T) {
	setServerViper(t, -1) // port -1 cannot be bound
	defer func() {
		if recover() == nil {
			t.Fatal("runServer must panic when the listener cannot be bound")
		}
	}()
	runServer()
}

func TestMainRejectsUnknownFlag(t *testing.T) {
	oldArgs := os.Args
	oldExit := osExit
	defer func() { os.Args = oldArgs; osExit = oldExit }()
	os.Args = []string{"server", "--definitely-not-a-flag"}
	code := 0
	osExit = func(c int) { code = c }

	main()

	if code != 1 {
		t.Fatalf("exit code = %d, want 1 for an unknown flag", code)
	}
}
