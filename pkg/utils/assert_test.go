package utils

import (
	"errors"
	"testing"
)

// TestAssertWriteLength covers the write guard HeadHandler runs every
// connection write through: a nil error passes silently, any error panics.
func TestAssertWriteLength(t *testing.T) {
	AssertWriteLength(3, nil) // must not panic

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("a failed write must panic through AssertWriteLength")
		}
	}()
	AssertWriteLength(0, errors.New("broken pipe"))
}
