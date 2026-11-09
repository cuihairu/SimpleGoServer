// Package demo is the business half of the cmd/server binary. It owns the
// TCP demo's frame codec and shared protocol handler; the application
// assembly (cobra/viper/flags and reactor startup) stays in main.go and
// only hands this package the config values it has already resolved.
package demo

import (
	"encoding/json"
	"sync"

	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// closeOnce guards Close; the shared protocol handler is process-wide, so
// tearing it down must happen exactly once.
var closeOnce sync.Once

// protocol is the shared protocol handler for every connection: the
// subscription table must be process-wide, or clients subscribed to the
// same topic on different connections would never see each other's
// publishes.
var protocol = proto.NewProtocolHandler(echoAction)

// echoAction is the demo business semantics: it echoes the action name back
// with the raw JSON payload as a response envelope.
func echoAction(action string, data []byte) (any, error) {
	return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
}

// Initializer wires the frame codec and the shared protocol handler into
// every accepted connection.
func Initializer(p handler.Pipeline) error {
	_ = p.AddLast(
		proto.NewFrameCodec(),
		protocol,
	)
	return nil
}

// RequireHello applies the strict-hello gate to the shared protocol handler.
func RequireHello(strict bool) {
	protocol.RequireHello(strict)
}

// Close tears down the shared protocol handler exactly once.
func Close() {
	closeOnce.Do(protocol.Close)
}
