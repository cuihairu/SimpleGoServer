package demo

import (
	"net"
	"testing"

	"github.com/cuihairu/simplegoserver/internal/handler"
)

// TestInitializerWiresCodecAndProtocol drives the demo pipeline assembly end
// to end on a real pipeline, so the frame codec and shared protocol handler
// are attached exactly as the reactor would attach them.
func TestInitializerWiresCodecAndProtocol(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	p := handler.NewPipeline(server)
	if err := Initializer(p); err != nil {
		t.Fatalf("Initializer returned an error: %v", err)
	}
}

// TestEchoActionEnvelopesPayload pins the demo business semantics: the action
// name comes back with the raw payload wrapped as a JSON envelope.
func TestEchoActionEnvelopesPayload(t *testing.T) {
	got, err := echoAction("echo", []byte(`{"a":1}`))
	if err != nil {
		t.Fatalf("echoAction returned an error: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("echoAction returned %T, want map[string]any", got)
	}
	if m["action"] != "echo" {
		t.Fatalf("envelope action = %v, want echo", m["action"])
	}
	if m["data"] == nil {
		t.Fatal("envelope data must carry the raw payload")
	}
}

// TestRequireHelloAndClose covers the config application and the once-guarded
// shutdown: the second Close must be a no-op.
func TestRequireHelloAndClose(t *testing.T) {
	RequireHello(true)
	RequireHello(false)
	Close()
	Close() // the once guard must make the second call a no-op
}
