// Package scripts_test's second file covers scripts/fuzz-smoke.sh.
//
// The coverage gate's tests (coverage_gate_test.go) and this one share a
// philosophy: a gate is only worth its runtime if it fails when it should.
// "Always exit 0" is the failure mode that matters, and it is the one a happy
// path can never detect -- so most of what follows drives the script with a
// stubbed `go` and asserts the *verdict*, not just the plumbing.
//
// Stubbing `go` rather than fuzzing for real is what makes this cheap and
// deterministic. A real crash can only be provoked by an input nobody has
// found yet; here the crash is simply "the stub exits 1 for this target",
// which is exactly the signal the script has to interpret correctly.
package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// fuzzScript resolves the fuzz gate relative to this source file, so the test
// does not care where `go test` was invoked from.
func fuzzScript(t *testing.T) string {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(self), "fuzz-smoke.sh")
}

// stubGo installs a fake `go` first on PATH. The stub appends every argv it
// receives to a log file, one invocation per line group, and exits 1 for any
// invocation whose argv mentions a target named in failOn (so a test can
// crash exactly one target and watch what the script does with the rest).
//
// The log is what lets a test assert the *shape* of the command -- that
// -fuzztime and -run are wired through, and, more importantly, that -race is
// absent, which is a documented design decision rather than an accident.
func stubGo(t *testing.T, logPath, failOn string) {
	t.Helper()
	dir := t.TempDir()
	stub := "#!/usr/bin/env bash\n" +
		"{\n" +
		"  printf 'INVOCATION\\n'\n" +
		"  printf '%s\\n' \"$@\"\n" +
		"} >> \"$FUZZ_STUB_LOG\"\n" +
		"if [ -n \"$FUZZ_STUB_FAIL\" ]; then\n" +
		"  for a in \"$@\"; do\n" +
		"    if [ \"$a\" = \"$FUZZ_STUB_FAIL\" ]; then exit 1; fi\n" +
		"  done\n" +
		"fi\n" +
		"echo 'ok  github.com/cuihairu/simplegoserver/pkg/proto 5.1s'\n"
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(stub), 0o700); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FUZZ_STUB_LOG", logPath)
	t.Setenv("FUZZ_STUB_FAIL", failOn)
}

// runFuzz executes the gate with bash and reports exit code plus output.
// Invoked through bash explicitly so a lost mode bit shows up as its own
// TestScriptIsExecutable failure instead of a wall of "permission denied".
func runFuzz(t *testing.T, script string, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{script}, args...)...)
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
	return code, string(out)
}

func TestFuzzSmokeScriptIsExecutable(t *testing.T) {
	info, err := os.Stat(fuzzScript(t))
	if err != nil {
		t.Fatalf("stat gate: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Error("fuzz-smoke.sh is not executable; CI invokes it directly")
	}
	raw, err := os.ReadFile(fuzzScript(t))
	if err != nil {
		t.Fatalf("read gate: %v", err)
	}
	if !strings.HasPrefix(string(raw), "#!") {
		t.Error("missing shebang; CI invokes it directly")
	}
}

// The happy path, and the one that keeps the gate from being disabled by a
// false alarm: with every target surviving, the verdict must be an explicit
// PASS whose count matches the number of targets actually run.
func TestFuzzSmokePassesWhenEveryTargetSurvives(t *testing.T) {
	stubGo(t, filepath.Join(t.TempDir(), "log"), "")

	code, out := runFuzz(t, fuzzScript(t))
	if code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	if !strings.Contains(out, "PASS") {
		t.Errorf("output missing PASS verdict\n%s", out)
	}
	for _, want := range []string{"FuzzDecodeHeader", "FuzzDecodeStream", "FuzzFrameRoundTrip"} {
		if !strings.Contains(out, want) {
			t.Errorf("output does not mention target %s\n%s", want, out)
		}
	}
	// The reported count must be derived from the runs, not hardcoded: if a
	// fourth target is added tomorrow, a stale "3 target(s)" would make the
	// summary a lie the tests happily accept.
	ran := strings.Count(out, "fuzz-smoke: Fuzz")
	reported := regexp.MustCompile(`PASS -- (\d+) target`).FindStringSubmatch(out)
	if reported == nil {
		t.Fatalf("output has no 'PASS -- N target(s)' line\n%s", out)
	}
	if n, err := strconv.Atoi(reported[1]); err != nil || n != ran {
		t.Errorf("reported %s targets but ran %d\n%s", reported[1], ran, out)
	}
}

// The failure direction is the whole point. A crash must produce a non-zero
// exit, name the target that died, and tell the reader where the crash input
// landed -- committing that file is what turns this find into a permanent
// regression case, so a gate that fails without saying so wastes the finding.
func TestFuzzSmokeFailsAndNamesTheCrashedTarget(t *testing.T) {
	// The stub matches on the exact -fuzz pattern the script builds for
	// FuzzDecodeStream, so the crash is attributed to the right target.
	stubGo(t, filepath.Join(t.TempDir(), "log"), "^FuzzDecodeStream$")

	code, out := runFuzz(t, fuzzScript(t))
	if code != 1 {
		t.Fatalf("exit = %d, want 1\n%s", code, out)
	}
	if !strings.Contains(out, "FAIL") {
		t.Errorf("output missing FAIL verdict\n%s", out)
	}
	if !strings.Contains(out, "testdata/fuzz/FuzzDecodeStream/") {
		t.Errorf("output does not point at the crash corpus path to commit\n%s", out)
	}
	if !strings.Contains(out, "FuzzDecodeStream CRASHED") {
		t.Errorf("output does not name the crashed target\n%s", out)
	}
	// One target dying must not hide the others' results: the surviving
	// targets still ran, so a reader learns the blast radius in one pass.
	if !strings.Contains(out, "FuzzDecodeHeader survived") {
		t.Errorf("surviving targets were not reported after a crash\n%s", out)
	}
	// And the healthy targets must not be blamed.
	if strings.Contains(out, "FuzzDecodeHeader CRASHED") || strings.Contains(out, "FuzzFrameRoundTrip CRASHED") {
		t.Errorf("a surviving target was reported as crashed\n%s", out)
	}
}

// The command shape is a set of decisions, so it is pinned. In particular
// -race must stay out: the detector cuts mutation throughput by roughly an
// order of magnitude, and the race step already owns that ground. -run '^$'
// is equally load-bearing -- without it every non-fuzz test in the package
// runs once per target and the budget stops being a fuzz budget.
func TestFuzzSmokeBuildsTheIntendedCommand(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "log")
	stubGo(t, logPath, "")

	if code, out := runFuzz(t, fuzzScript(t), "--fuzztime", "45s"); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("stub `go` was never invoked: %v", err)
	}
	log := string(raw)
	for _, want := range []string{"-run", "^$", "-fuzz", "-fuzztime", "45s", "./pkg/proto"} {
		if !strings.Contains(log, want) {
			t.Errorf("argv missing %q\n%s", want, log)
		}
	}
	if !strings.Contains(log, "-fuzz") || !strings.Contains(log, "^FuzzDecodeHeader$") {
		t.Errorf("each target must be fuzzed under its own anchored name\n%s", log)
	}
	if strings.Contains(log, "-race") {
		t.Errorf("the fuzz budget must not be spent under the race detector\n%s", log)
	}
	// Anchoring matters: an unanchored ^FuzzX$ pattern would also match a
	// future FuzzXxx, silently splitting one target's budget across two.
	if !regexp.MustCompile(`-fuzz\n\^[A-Za-z0-9_]+\$\n`).MatchString(log) {
		t.Errorf("-fuzz patterns must be fully anchored (^Name$)\n%s", log)
	}
}

// A default run needs no arguments, and the default budget is the one CI
// runs; pinning it keeps local and CI runs comparable.
func TestFuzzSmokeDefaultBudget(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "log")
	stubGo(t, logPath, "")

	if code, out := runFuzz(t, fuzzScript(t)); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("stub `go` was never invoked: %v", err)
	}
	if !strings.Contains(string(raw), "20s") {
		t.Errorf("default budget is not 20s per target\n%s", raw)
	}
}

// Discovery must see targets the moment they exist, and must not invent
// targets that the toolchain would refuse to run. Both halves matter: a
// hardcoded CI list rots the moment someone forgets it, and a too-greedy
// pattern produces a target name that cannot be fuzzed.
//
// The fixture is a throwaway repo (the script derives its root from its own
// location), which also proves the gate works from any directory.
func TestFuzzSmokeDiscoversRealTargetsOnly(t *testing.T) {
	repo := t.TempDir()
	pkgDir := filepath.Join(repo, "pkg", "codec")
	if err := os.MkdirAll(pkgDir, 0o750); err != nil {
		t.Fatal(err)
	}
	real := "package codec\n\n" +
		"import \"testing\"\n\n" +
		"func FuzzRealTarget(f *testing.F) { f.Fuzz(func(t *testing.T, b []byte) {}) }\n" +
		// A decoy: fuzz-shaped name, wrong signature. The toolchain would
		// not accept it as a target, so neither may the gate.
		"func FuzzDecoyHelper(t *testing.T) {}\n"
	if err := os.WriteFile(filepath.Join(pkgDir, "codec_test.go"), []byte(real), 0o600); err != nil {
		t.Fatal(err)
	}
	// A crash corpus under testdata/ mentions a target name too; it is
	// already replayed by the plain `go test` seed run and must not be
	// rediscovered as a source-level target.
	corpus := filepath.Join(repo, "pkg", "codec", "testdata", "fuzz", "FuzzStale")
	if err := os.MkdirAll(corpus, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corpus, "deadbeef"), []byte("go test fuzz v1\n[]byte{1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	scriptDir := filepath.Join(repo, "scripts")
	if err := os.MkdirAll(scriptDir, 0o750); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(fuzzScript(t))
	if err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(scriptDir, "fuzz-smoke.sh")
	if err := os.WriteFile(local, body, 0o750); err != nil {
		t.Fatal(err)
	}

	stubGo(t, filepath.Join(t.TempDir(), "log"), "")
	code, out := runFuzz(t, local, "--list")
	if code != 0 {
		t.Fatalf("--list exit = %d, want 0\n%s", code, out)
	}
	if !strings.Contains(out, "pkg/codec\tFuzzRealTarget") {
		t.Errorf("the real target was not discovered\n%s", out)
	}
	if strings.Contains(out, "FuzzDecoyHelper") {
		t.Errorf("a non-target with a fuzz-shaped name was discovered\n%s", out)
	}
	if strings.Contains(out, "FuzzStale") {
		t.Errorf("a crash corpus was rediscovered as a source target\n%s", out)
	}
}

// A repo with no fuzz targets must fail, not pass. "0 targets, 0 crashes" is
// the most dangerous green a gate like this can produce: it survives every
// future commit while checking nothing. The fixture is again a throwaway repo
// so the real one is unaffected.
func TestFuzzSmokeFailsWhenNoTargetsExist(t *testing.T) {
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "plain_test.go"),
		[]byte("package plain\n\nimport \"testing\"\n\nfunc TestNothing(t *testing.T) {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	scriptDir := filepath.Join(repo, "scripts")
	if err := os.MkdirAll(scriptDir, 0o750); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(fuzzScript(t))
	if err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(scriptDir, "fuzz-smoke.sh")
	if err := os.WriteFile(local, body, 0o750); err != nil {
		t.Fatal(err)
	}

	stubGo(t, filepath.Join(t.TempDir(), "log"), "")
	code, out := runFuzz(t, local)
	if code != 1 {
		t.Fatalf("exit = %d, want 1 for a repo with no fuzz targets\n%s", code, out)
	}
	if !strings.Contains(out, "no fuzz targets found") {
		t.Errorf("output does not explain why nothing was checked\n%s", out)
	}
	// It must not have invoked `go` at all: failing here is about the
	// discovery, not about a target.
	if _, err := os.Stat(os.Getenv("FUZZ_STUB_LOG")); err == nil {
		t.Error("go was invoked despite there being no targets")
	}
}

// Misuse must be distinguishable from a finding: exit 2, not 1, so CI does
// not report "a target crashed" for a typo. --fuzztime is forwarded to
// `go test -fuzztime`, so its shape is validated here rather than surfacing
// as a confusing flag error halfway through a run.
//
// The iteration form `20x` is a real `go test` syntax, not a typo -- which is
// exactly why it needs its own case: it parses fine and then quietly makes the
// budget machine-dependent.
func TestFuzzSmokeUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"unknown argument", []string{"--wat"}},
		{"iteration-count fuzztime", []string{"--fuzztime", "20x"}},
		{"junk in fuzztime", []string{"--fuzztime", "20s!"}},
		{"fuzztime with no digits", []string{"--fuzztime", "abc"}},
		{"empty fuzztime", []string{"--fuzztime", ""}},
		{"dangling fuzztime", []string{"--fuzztime"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubGo(t, filepath.Join(t.TempDir(), "log"), "")
			if code, out := runFuzz(t, fuzzScript(t), tc.args...); code != 2 {
				t.Errorf("exit = %d, want 2\n%s", code, out)
			}
		})
	}
}

func TestFuzzSmokeHelp(t *testing.T) {
	code, out := runFuzz(t, fuzzScript(t), "--help")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	for _, want := range []string{"--fuzztime", "--list", "Exit status"} {
		if !strings.Contains(out, want) {
			t.Errorf("help does not mention %s\n%s", want, out)
		}
	}
	// The help text is the header comment, nothing more. Leaking shell
	// source into it (an off-by-one in the sed range) makes the usage
	// unreadable, so the boundary is asserted rather than eyeballed.
	for _, leak := range []string{"set -euo", "while [", "discover()", "IFS="} {
		if strings.Contains(out, leak) {
			t.Errorf("help leaks script source (%q)\n%s", leak, out)
		}
	}
}
