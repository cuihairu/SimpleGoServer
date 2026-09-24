package pkg

import (
	"errors"
	"testing"
)

type testOptions struct {
	value int
	name  string
}

func TestApplyRunsOptionsInOrderAndStopsAtFirstError(t *testing.T) {
	opts := &testOptions{}
	var order []string

	err := Apply(opts,
		func(o *testOptions) error {
			o.value = 1
			order = append(order, "first")
			return nil
		},
		func(o *testOptions) error {
			o.name = "second"
			order = append(order, "second")
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Apply(): %v", err)
	}
	if opts.value != 1 || opts.name != "second" {
		t.Fatalf("options = %+v, want both applied", opts)
	}
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("order = %v, want in-sequence application", order)
	}

	boom := errors.New("boom")
	after := false
	err = Apply(opts,
		func(o *testOptions) error { return boom },
		func(o *testOptions) error { after = true; return nil },
	)
	if !errors.Is(err, boom) {
		t.Fatalf("Apply() error = %v, want boom", err)
	}
	if after {
		t.Fatal("options after the failing one must not run")
	}
}

func TestWithComposesOptions(t *testing.T) {
	opts := &testOptions{}
	inner := With(
		func(o *testOptions) error { o.value = 7; return nil },
		func(o *testOptions) error { o.name = "composed"; return nil },
	)
	if err := Apply(opts, inner); err != nil {
		t.Fatalf("Apply(composed): %v", err)
	}
	if opts.value != 7 || opts.name != "composed" {
		t.Fatalf("options = %+v, want the composed values", opts)
	}
}
