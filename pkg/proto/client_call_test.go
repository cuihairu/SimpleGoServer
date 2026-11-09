package proto

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"
)

func TestClientEndToEndConcurrentCalls(t *testing.T) {
	addr, _ := startTCPServer(t, echoHandler)
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	const goroutines = 8
	const calls = 25
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < calls; i++ {
				want := "hello"
				resp, err := client.Call("echo", want, 5*time.Second)
				if err != nil {
					errs <- err
					return
				}
				var body struct {
					Action string          `json:"action"`
					Data   json.RawMessage `json:"data"`
				}
				if err := json.Unmarshal(resp.Data, &body); err != nil {
					errs <- err
					return
				}
				var echoed string
				if err := json.Unmarshal(body.Data, &echoed); err != nil {
					errs <- err
					return
				}
				if echoed != want {
					errs <- testError("cross-wired response: " + echoed)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestClientSubscribeReceivesPublish(t *testing.T) {
	addr, ph := startTCPServer(t, echoHandler)

	pushes := make(chan *Frame, 8)
	client, err := Dial(addr, func(frame *Frame) { pushes <- frame })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	if err := client.Subscribe("alerts", 5*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	if _, err := ph.Publish("alerts", map[string]string{"n": "1"}); err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	select {
	case frame := <-pushes:
		if frame.Header.FrameType != PUBLISH {
			t.Fatalf("event type = %s, want PUBLISH", frame.Header.FrameType)
		}
		msg, _ := DecodeJSONMessage(frame)
		if msg.Action != "alerts" {
			t.Fatalf("event topic = %q, want alerts", msg.Action)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no publish received")
	}

	if err := client.Unsubscribe("alerts", 5*time.Second); err != nil {
		t.Fatalf("Unsubscribe(): %v", err)
	}
	_, _ = ph.Publish("alerts", "gone")
	select {
	case frame := <-pushes:
		t.Fatalf("unexpected push after unsubscribe: %s", frame)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestClientCallTimeout(t *testing.T) {
	slow := func(action string, data []byte) (any, error) {
		time.Sleep(300 * time.Millisecond)
		return "done", nil
	}
	addr, _ := startTCPServer(t, slow)
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	if _, err := client.Call("echo", nil, 50*time.Millisecond); err == nil {
		t.Fatal("expected timeout error")
	}
	// a late response must not corrupt the next call's correlation
	resp, err := client.Call("echo", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("call after timeout: %v", err)
	}
	if string(resp.Data) != `"done"` {
		t.Fatalf("data = %s", resp.Data)
	}
}

func TestClientPendingFailOnDisconnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	client, err := Dial(listener.Addr().String(), nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	serverSide := <-accepted
	// hold a call open, then kill the server side
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Call("echo", nil, 10*time.Second)
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	_ = serverSide.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after disconnect")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call did not fail after disconnect")
	}
}

func TestClientPingAndGracefulClose(t *testing.T) {
	addr, _ := startTCPServer(t, echoHandler)
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()
	if err := client.Ping(5 * time.Second); err != nil {
		t.Fatalf("Ping(): %v", err)
	}
	if err := client.CloseGracefully(5 * time.Second); err != nil {
		t.Fatalf("CloseGracefully(): %v", err)
	}
	// idempotent
	if err := client.CloseGracefully(time.Second); err != nil {
		t.Fatalf("second CloseGracefully(): %v", err)
	}
}

func TestClientCallAfterClose(t *testing.T) {
	addr, _ := startTCPServer(t, echoHandler)
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if _, err := client.Call("echo", nil, time.Second); err != ErrClosed {
		t.Fatalf("Call after close = %v, want ErrClosed", err)
	}
}
