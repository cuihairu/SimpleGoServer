// Package scripts_test covers scripts/check-coverage.sh.
//
// The gate's whole value is that it fails when coverage regresses. A script
// that always exits 0 would also print a reassuring table, so these tests
// assert the failure direction just as carefully as the success direction:
// each one feeds a synthetic profile with a deliberate gap and requires a
// non-zero exit naming the offending package.
//
// Synthetic profiles rather than a real `go test` run, for two reasons: it
// keeps the test at milliseconds, and -- more importantly -- a gap can be
// placed exactly, so the arithmetic on the threshold boundary is exercised
// deterministically instead of hoping the real repo happens to sit there.
package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// scriptPath resolves the gate relative to this source file rather than the
// working directory, so the test does not care where `go test` was invoked.
func scriptPath(t *testing.T) string {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(self), "check-coverage.sh")
}

// block is one coverage-profile record: a range of statements plus whether
// that range ever executed.
//
// Two format details drive the whole fixture design, and both were learned
// by getting them wrong first:
//
//  1. A profile line is "<import path>/<file>.go:<range>", so the package is
//     everything *except* the last path segment. pkg and file are separate
//     fields so a fixture cannot collapse two intended packages into one.
//
//  2. Coverage is per *block*, and a block is all-or-nothing: count > 0 means
//     every statement in that range counts as covered. A range that ran four
//     times is still 100% covered. So a partial package is expressed as two
//     blocks -- one covered, one not -- never as "10 statements, 4 covered",
//     which is simply not a thing the format can say.
type block struct {
	pkg     string
	file    string
	stmts   int
	covered bool
}

// covered is a shorthand for a fully executed block.
func covered(pkg, file string, stmts int) block {
	return block{pkg: pkg, file: file, stmts: stmts, covered: true}
}

// uncovered is a shorthand for a never-executed block.
func uncovered(pkg, file string, stmts int) block {
	return block{pkg: pkg, file: file, stmts: stmts, covered: false}
}

// writeProfile emits a syntactically valid coverprofile. The first line is
// the mode header the real tool writes; the parser skips it.
func writeProfile(t *testing.T, path string, blocks ...block) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("mode: atomic\n")
	for _, b := range blocks {
		count := "0"
		if b.covered {
			count = "1"
		}
		sb.WriteString(b.pkg)
		sb.WriteByte('/')
		sb.WriteString(b.file)
		sb.WriteString(":1.1,2.1 ")
		sb.WriteString(strconv.Itoa(b.stmts))
		sb.WriteByte(' ')
		sb.WriteString(count)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(sb.String()), 0o600); err != nil {
		t.Fatalf("write profile: %v", err)
	}
	return path
}

type result struct {
	exitCode int
	output   string
}

func runGate(t *testing.T, args ...string) result {
	t.Helper()
	// Invoked through bash explicitly: the exec bit is asserted separately by
	// TestScriptIsExecutable, and a mode bit lost in a clone should not turn
	// every one of these tests into a confusing "permission denied".
	cmd := exec.Command("bash", append([]string{scriptPath(t)}, args...)...)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if e, ok := err.(*exec.ExitError); ok {
			exitErr = e
			code = exitErr.ExitCode()
		} else {
			t.Fatalf("running gate: %v", err)
		}
	}
	return result{exitCode: code, output: string(out)}
}

func TestScriptIsExecutable(t *testing.T) {
	path := scriptPath(t)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat gate: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Error("check-coverage.sh is not executable; CI invokes it directly")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read gate: %v", err)
	}
	if !strings.HasPrefix(string(raw), "#!") {
		t.Error("missing shebang; CI invokes it directly")
	}
}

// A profile with no gap must pass. This is the regression guard for the
// gate's own false-positive risk: a gate that cries wolf gets disabled.
func TestGatePassesWhenFullyCovered(t *testing.T) {
	profile := writeProfile(t, filepath.Join(t.TempDir(), "cov.out"),
		covered("example.com/app/core", "a.go", 10),
		covered("example.com/app/core", "b.go", 4),
	)

	got := runGate(t, "--profile", profile)
	if got.exitCode != 0 {
		t.Fatalf("exit = %d, want 0\n%s", got.exitCode, got.output)
	}
	for _, want := range []string{
		"example.com/app/core",
		"14/14",
		"100.00%",
		"PASS",
	} {
		if !strings.Contains(got.output, want) {
			t.Errorf("output missing %q\n%s", want, got.output)
		}
	}
}

// The suite-mode run must carry -count=1, or the gate can certify a stale
// profile. `go test` caches results, so a warm GOCACHE (CI restores one via
// setup-go's default `cache: true`) turns this command into "(cached)" plus a
// profile from the previous run -- green, and meaningless.
//
// --profile mode cannot observe this, since it never invokes `go test`, so the
// assertion is made on the invocation itself: a stub `go` first on PATH records
// its argv and writes a passing profile. The stub also proves the gate still
// reaches `go test` at all, so the test cannot pass vacuously by the gate
// skipping the run.
func TestSuiteModeRunDisablesResultCache(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "argv")
	stub := "#!/usr/bin/env bash\n" +
		"printf '%s\\n' \"$@\" > \"$GATE_STUB_ARGS\"\n" +
		"for a in \"$@\"; do\n" +
		"  case \"$a\" in\n" +
		"  -coverprofile=*) printf 'mode: atomic\\nexample.com/app/core/a.go:1.1,2.1 3 1\\n' > \"${a#-coverprofile=}\" ;;\n" +
		"  esac\n" +
		"done\n"
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(stub), 0o700); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cmd := exec.Command("bash", scriptPath(t))
	cmd.Env = append(os.Environ(),
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GATE_STUB_ARGS="+argsPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("gate failed with stubbed go: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "PASS") {
		t.Fatalf("gate did not pass on the stub's profile\n%s", out)
	}

	raw, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("stub `go` was never invoked (no argv recorded): %v", err)
	}
	if !strings.Contains(string(raw), "-count=1\n") {
		t.Errorf("suite-mode `go test` must pass -count=1 to defeat the test "+
			"result cache, argv was:\n%s", raw)
	}
}

// A block that merely ran more than once is still fully covered. Pinned
// because the profile format's all-or-nothing semantics make this the easiest
// thing to misread when writing a gate.
func TestGateCountsRepeatedExecutionAsCovered(t *testing.T) {
	profile := writeProfile(t, filepath.Join(t.TempDir(), "cov.out"),
		block{pkg: "example.com/app/x", file: "a.go", stmts: 10, covered: true},
	)
	if got := runGate(t, "--profile", profile); got.exitCode != 0 {
		t.Fatalf("exit = %d, want 0\n%s", got.exitCode, got.output)
	}
}

// The core case: a partially covered package must fail and be named. Without
// the package name the failure would be unactionable.
func TestGateFailsOnPartialCoverage(t *testing.T) {
	profile := writeProfile(t, filepath.Join(t.TempDir(), "cov.out"),
		covered("example.com/app/whole", "a.go", 10), // 10/10
		covered("example.com/app/gappy", "b.go", 6),  // 6/10 -> 60%
		uncovered("example.com/app/gappy", "c.go", 4),
	)

	got := runGate(t, "--profile", profile)
	if got.exitCode != 1 {
		t.Fatalf("exit = %d, want 1\n%s", got.exitCode, got.output)
	}
	if !strings.Contains(got.output, "FAIL") {
		t.Errorf("output missing FAIL verdict\n%s", got.output)
	}
	if !strings.Contains(got.output, "example.com/app/gappy") {
		t.Errorf("output does not name the offending package\n%s", got.output)
	}
	if !strings.Contains(got.output, "60.00%") {
		t.Errorf("output does not show the computed percentage\n%s", got.output)
	}
	if !strings.Contains(got.output, "below 100.0%") {
		t.Errorf("output does not report the threshold it breached\n%s", got.output)
	}
	// The healthy package must not be blamed. The flag line is the offender's
	// package name followed by padding and a caret.
	if strings.Contains(got.output, "whole") && strings.Contains(got.output, "whole  ^") {
		t.Errorf("a fully covered package was flagged\n%s", got.output)
	}
}

// A repo-wide total can hide one bad package behind a big healthy one. The
// gate's unit is the package, so this must still fail -- pinned explicitly
// with a lopsided split.
func TestGateFailsDespiteHealthyRepoTotal(t *testing.T) {
	profile := writeProfile(t, filepath.Join(t.TempDir(), "cov.out"),
		covered("example.com/app/big", "big.go", 1000),  // 1000/1000
		uncovered("example.com/app/tiny", "tiny.go", 1), // 0/1 -> 99.90% total
	)

	got := runGate(t, "--profile", profile)
	if got.exitCode != 1 {
		t.Fatalf("exit = %d, want 1 (99.90%% total must not pass)\n%s", got.exitCode, got.output)
	}
	if !strings.Contains(got.output, "example.com/app/tiny") {
		t.Errorf("output does not name the offending package\n%s", got.output)
	}
}

// A package whose statements are all uncovered is 0%, and must fail just as
// loudly as a partial gap.
func TestGateFailsOnFullyUncoveredPackage(t *testing.T) {
	profile := writeProfile(t, filepath.Join(t.TempDir(), "cov.out"),
		uncovered("example.com/app/never", "never.go", 7),
	)

	got := runGate(t, "--profile", profile)
	if got.exitCode != 1 {
		t.Fatalf("exit = %d, want 1\n%s", got.exitCode, got.output)
	}
	if !strings.Contains(got.output, "0/7") {
		t.Errorf("output does not show the 0/7 counts\n%s", got.output)
	}
	if !strings.Contains(got.output, "0.00%") {
		t.Errorf("output does not show 0.00%%\n%s", got.output)
	}
}

// A gap of a single statement out of 1000 must be caught. This is the case
// that most plausibly slips through a hand-rolled check, and the one the
// gate exists for.
func TestGateCatchesSingleStatementGap(t *testing.T) {
	profile := writeProfile(t, filepath.Join(t.TempDir(), "cov.out"),
		covered("example.com/app/almost", "a.go", 999),
		uncovered("example.com/app/almost", "b.go", 1),
	)

	got := runGate(t, "--profile", profile)
	if got.exitCode != 1 {
		t.Fatalf("exit = %d, want 1 for a 99.90%% package\n%s", got.exitCode, got.output)
	}
	if !strings.Contains(got.output, "999/1000") {
		t.Errorf("output does not show the 999/1000 counts\n%s", got.output)
	}
	if !strings.Contains(got.output, "99.90%") {
		t.Errorf("output does not show the computed percentage\n%s", got.output)
	}
}

// Pure interface packages (pkg/event in this repo) have no statements at
// all. They are skipped rather than counted as 0%, which would make every
// run fail for a reason no test could fix.
func TestGateSkipsPackagesWithoutStatements(t *testing.T) {
	dir := t.TempDir()
	// A profile whose only entry has zero statements must be reported as
	// "nothing to check" and fail, not silently pass or crash.
	empty := writeProfile(t, filepath.Join(dir, "empty.out"),
		covered("example.com/app/interfaces", "interfaces.go", 0),
	)
	got := runGate(t, "--profile", empty)
	if got.exitCode != 1 {
		t.Fatalf("exit = %d, want 1 for a profile with no statements\n%s", got.exitCode, got.output)
	}
	if !strings.Contains(got.output, "no packages with statements") {
		t.Errorf("output does not explain why nothing was checked\n%s", got.output)
	}

	// And a healthy profile passes with no trace of the statementless entry,
	// which never appears because the real run over ./... always has others.
	real := writeProfile(t, filepath.Join(dir, "cov.out"),
		covered("example.com/app/real", "real.go", 5),
	)
	mixed := runGate(t, "--profile", real)
	if mixed.exitCode != 0 {
		t.Fatalf("exit = %d, want 0\n%s", mixed.exitCode, mixed.output)
	}
}

// The threshold is configurable, so a partially covered package must pass
// once the floor is relaxed below its actual coverage -- and a package
// sitting exactly on the threshold must pass, not fail on float noise.
func TestGateThresholdOverride(t *testing.T) {
	profile := writeProfile(t, filepath.Join(t.TempDir(), "cov.out"),
		covered("example.com/app/gappy", "a.go", 6),
		uncovered("example.com/app/gappy", "b.go", 4), // 60.00%
	)

	if got := runGate(t, "--profile", profile, "--threshold", "60"); got.exitCode != 0 {
		t.Errorf("exit = %d at threshold 60, want 0 (exactly 60%%)\n%s", got.exitCode, got.output)
	}
	if got := runGate(t, "--profile", profile, "--threshold", "59.9"); got.exitCode != 0 {
		t.Errorf("exit = %d at threshold 59.9, want 0\n%s", got.exitCode, got.output)
	}
	if got := runGate(t, "--profile", profile, "--threshold", "50"); got.exitCode != 0 {
		t.Errorf("exit = %d at threshold 50, want 0 (60%% clears 50%%)\n%s", got.exitCode, got.output)
	}
	// The other side of the boundary: 70 is above the package's 60%, so it fails.
	if got := runGate(t, "--profile", profile, "--threshold", "70"); got.exitCode != 1 {
		t.Errorf("exit = %d at threshold 70, want 1\n%s", got.exitCode, got.output)
	}
	// The = form must behave identically to the space-separated form.
	if got := runGate(t, "--profile="+profile, "--threshold=70"); got.exitCode != 1 {
		t.Errorf("exit = %d with --flag=value form, want 1\n%s", got.exitCode, got.output)
	}
}

// Misuse must be distinguishable from a coverage failure: exit 2, not 1, so
// CI does not report "coverage dropped" for a typo.
func TestGateUsageErrors(t *testing.T) {
	dir := t.TempDir()
	profile := writeProfile(t, filepath.Join(dir, "cov.out"), covered("example.com/app/a", "a.go", 3))

	cases := []struct {
		name string
		args []string
	}{
		{"missing profile file", []string{"--profile", filepath.Join(dir, "nope.out")}},
		{"unknown argument", []string{"--wat"}},
		{"non-numeric threshold", []string{"--profile", profile, "--threshold", "high"}},
		{"empty threshold", []string{"--profile", profile, "--threshold", ""}},
		{"dangling --threshold", []string{"--profile", profile, "--threshold"}},
		{"dangling --profile", []string{"--profile"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runGate(t, tc.args...)
			if got.exitCode != 2 {
				t.Errorf("exit = %d, want 2\n%s", got.exitCode, got.output)
			}
		})
	}
}

func TestGateHelp(t *testing.T) {
	got := runGate(t, "--help")
	if got.exitCode != 0 {
		t.Fatalf("exit = %d, want 0\n%s", got.exitCode, got.output)
	}
	for _, want := range []string{"--threshold", "--profile"} {
		if !strings.Contains(got.output, want) {
			t.Errorf("help does not mention %s\n%s", want, got.output)
		}
	}
}

// Deterministic output: `for (p in arr)` in awk has unspecified order, and
// these lines are what a human reads when the gate fails. Pin the ordering
// so the report is stable across runs and machines.
func TestGateOrdersPackagesDeterministically(t *testing.T) {
	profile := writeProfile(t, filepath.Join(t.TempDir(), "cov.out"),
		covered("example.com/app/zeta", "zeta.go", 1),
		covered("example.com/app/alpha", "alpha.go", 1),
		covered("example.com/app/mid", "mid.go", 1),
	)

	first := runGate(t, "--profile", profile).output
	for i := 0; i < 3; i++ {
		if got := runGate(t, "--profile", profile).output; got != first {
			t.Fatalf("output is not stable across runs:\nfirst:\n%s\nrun %d:\n%s", first, i, got)
		}
	}
	alpha := strings.Index(first, "app/alpha")
	mid := strings.Index(first, "app/mid")
	zeta := strings.Index(first, "app/zeta")
	if alpha < 0 || mid < 0 || zeta < 0 || !(alpha < mid && mid < zeta) {
		t.Errorf("packages not in sorted order:\n%s", first)
	}
}
