package reactor

import (
	"runtime"
	"testing"
)

// TestServerOptionsDefaults pins the accessor normalization: invalid or
// unconstrained worker counts collapse to NumCPU, and an empty listener
// falls back to the default address.
func TestServerOptionsDefaults(t *testing.T) {
	o := NewServerOptions()

	if got := o.GetNumWorkers(); got != runtime.NumCPU() {
		t.Fatalf("GetNumWorkers() = %d, want NumCPU (%d)", got, runtime.NumCPU())
	}

	o.NumWorkers = 2
	if got := o.GetNumWorkers(); got != 2 {
		t.Fatalf("GetNumWorkers() = %d, want the explicit 2", got)
	}

	o.NumWorkers = runtime.NumCPU() + 10
	if got := o.GetNumWorkers(); got != runtime.NumCPU() {
		t.Fatalf("GetNumWorkers() above NumCPU = %d, want it clamped to NumCPU", got)
	}

	o.NumWorkers = 1
	o.Multicore = true
	if got := o.GetNumWorkers(); got != runtime.NumCPU() {
		t.Fatalf("Multicore GetNumWorkers() = %d, want NumCPU", got)
	}

	o = NewServerOptions()
	o.Listener = ""
	if got := o.GetListener(); got != "127.0.0.1:8080" {
		t.Fatalf("GetListener() = %q, want the default address", got)
	}

	if o.GetLockThread() != false || o.GetMulticore() != false {
		t.Fatal("fresh options must report false for LockThread and Multicore")
	}
	o.Multicore = true
	o.LockThread = true
	if !o.GetMulticore() || !o.GetLockThread() {
		t.Fatal("accessors must report the set flags")
	}
}
