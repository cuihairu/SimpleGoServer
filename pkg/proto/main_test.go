package proto

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind: the
// protocol layer spawns janitors, keepalive loops, reconnect loops and
// read loops, and each of them must shut down with the object that owns it.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
