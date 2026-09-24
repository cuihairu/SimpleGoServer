package utils

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
