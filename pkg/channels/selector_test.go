package channels

import "testing"

func TestSelectKeysString(t *testing.T) {
	tests := []struct {
		keys SelectKeys
		want string
	}{
		{0, ""},
		{OP_READ, "OP_READ"},
		{OP_READ | OP_WRITE, "OP_READ|OP_WRITE"},
		{OP_ACCEPT | OP_CONNECT | OP_READ | OP_WRITE, "OP_READ|OP_WRITE|OP_CONNECT|OP_ACCEPT"},
	}
	for _, tt := range tests {
		if got := tt.keys.String(); got != tt.want {
			t.Errorf("SelectKeys(%d).String() = %q, want %q", tt.keys, got, tt.want)
		}
	}
}

func TestSelectKeysHasAddRemove(t *testing.T) {
	keys := SelectKeys(0)
	if keys.Has(OP_READ) {
		t.Fatal("empty keys must not have OP_READ")
	}

	keys = keys.Add(OP_READ).Add(OP_WRITE)
	if !keys.Has(OP_READ) || !keys.Has(OP_WRITE) {
		t.Fatalf("keys = %s, want OP_READ and OP_WRITE set", keys)
	}
	if keys.Has(OP_CONNECT) {
		t.Fatalf("keys = %s, must not have OP_CONNECT", keys)
	}

	keys = keys.Remove(OP_READ)
	if keys.Has(OP_READ) {
		t.Fatalf("keys = %s, OP_READ should be removed", keys)
	}
	if !keys.Has(OP_WRITE) {
		t.Fatalf("keys = %s, OP_WRITE must survive the removal of OP_READ", keys)
	}

	// Add is idempotent and Remove of an absent bit is a no-op
	if got := keys.Add(OP_WRITE); got != keys {
		t.Fatalf("Add of a present bit changed the keys: %s -> %s", keys, got)
	}
	if got := keys.Remove(OP_CONNECT); got != keys {
		t.Fatalf("Remove of an absent bit changed the keys: %s -> %s", keys, got)
	}
}
