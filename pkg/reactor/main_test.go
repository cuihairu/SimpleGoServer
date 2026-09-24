package reactor

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind: the
// reactor spawns accept/worker loops and per-connection handlers, and a
// leaked one usually means a shutdown path did not drain everything. The
// shared echo protocol handler's janitor is stopped first, since it serves
// every test reactor and outlives them by design.
func TestMain(m *testing.M) {
	code := m.Run()
	echoProtocol.Close()
	if err := goleak.Find(); err != nil {
		fmt.Fprintf(os.Stderr, "goleak: %v\n", err)
		code = 1
	}
	os.Exit(code)
}
