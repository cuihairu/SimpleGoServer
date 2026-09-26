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

// stubGo installs a fake `go` first on PATH. It emulates the three shapes
// the gate invokes:
//
//	go list ./...                            -> FUZZ_STUB_PKGS (one per line)
//	go test -list 'Fuzz.*' <pkg>             -> targets of <pkg> per
//	                                           FUZZ_STUB_TARGETS ("pkg<TAB>target")
//	go test <pkg> -run '^$' -fuzz ... ...    -> a fuzz run
//
// The stub appends every argv it receives to a log file, one invocation per
// line group, and exits 1 for any fuzz run whose argv mentions a target
// named in failOn (so a test can crash exactly one target and watch what the
// script does with the rest).
//
// The log is what lets a test assert the *shape* of the command -- that
// -fuzztime and -run are wired through, that each target is fuzzed in its
// own package, and, more importantly, that -race is absent, which is a
// documented design decision rather than an accident.
func stubGo(t *testing.T, logPath, failOn string) {
	t.Helper()
	dir := t.TempDir()
	// The package is always the final argument in both shapes above, so the
	// stub can attribute a -list invocation to a package without a real
	// argument parser.
	stub := "#!/usr/bin/env bash\n" +
		"{\n" +
		"  printf 'INVOCATION\\n'\n" +
		"  printf '%s\\n' \"$@\"\n" +
		"} >> \"$FUZZ_STUB_LOG\"\n" +
		"if [ \"$1\" = list ]; then printf '%s\\n' \"$FUZZ_STUB_PKGS\"; exit 0; fi\n" +
		"pkg=\"${@: -1}\"\n" +
		"saw_list=0\n" +
		"for a in \"$@\"; do [ \"$a\" = -list ] && saw_list=1; done\n" +
		"if [ \"$saw_list\" -eq 1 ]; then\n" +
		"  while IFS=\"$(printf '\\t')\" read -r p t; do\n" +
		"    [ \"$p\" = \"$pkg\" ] && printf '%s\\n' \"$t\"\n" +
		"  done <<< \"$FUZZ_STUB_TARGETS\"\n" +
		"  printf 'ok  %s 0.05s\\n' \"$pkg\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ -n \"$FUZZ_STUB_FAIL\" ]; then\n" +
		"  for a in \"$@\"; do\n" +
		"    if [ \"$a\" = \"$FUZZ_STUB_FAIL\" ]; then exit 1; fi\n" +
		"  done\n" +
		"fi\n" +
		"echo \"ok  $pkg 5.1s\"\n"
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(stub), 0o700); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FUZZ_STUB_LOG", logPath)
	t.Setenv("FUZZ_STUB_FAIL", failOn)
	t.Setenv("FUZZ_STUB_PKGS", strings.Join(stubPackages, "\n"))
	t.Setenv("FUZZ_STUB_TARGETS", strings.Join(stubTargetLines, "\n"))
}

// The stubbed world: two packages, three targets between them. Two packages
// rather than one so a test can catch an attribute-to-the-wrong-package bug,
// which a single-package world cannot express.
var (
	stubPackages    = []string{"./pkg/proto", "./pkg/codec"}
	stubTargetLines = []string{
		"./pkg/proto\tFuzzDecodeHeader",
		"./pkg/proto\tFuzzDecodeStream",
		"./pkg/codec\tFuzzFrameRoundTrip",
	}
)

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
	// Discovery is per package, so the run must be too: a target found in
	// one package and fuzzed in another is a package-level lie -- it either
	// "passes" against a package that has no such target, or crashes against
	// one that does.
	for _, want := range []string{"./pkg/proto\n-run\n^$\n-fuzz\n^FuzzDecodeHeader$", "./pkg/codec\n-run\n^$\n-fuzz\n^FuzzFrameRoundTrip$"} {
		if !strings.Contains(log, want) {
			t.Errorf("target was not fuzzed in its own package; wanted %s\n%s",
				strings.ReplaceAll(want, "\n", " "), log)
		}
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

// fixtureRepo writes a throwaway module plus a copy of the gate, and returns
// the path to that copy.
//
// The gate is *copied* rather than invoked in place because it derives its
// root from its own location -- only a copy inside the fixture makes it
// enumerate the fixture. That is deliberate: it also proves the gate works
// from a foreign tree, not just from the repo it grew up in.
func fixtureRepo(t *testing.T, files map[string]string) string {
	t.Helper()
	repo := t.TempDir()
	write := func(rel, body string, mode os.FileMode) {
		path := filepath.Join(repo, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.com/fixture\n\ngo 1.22\n", 0o600)
	for rel, body := range files {
		write(rel, body, 0o600)
	}
	body, err := os.ReadFile(fuzzScript(t))
	if err != nil {
		t.Fatalf("read gate: %v", err)
	}
	write("scripts/fuzz-smoke.sh", string(body), 0o750)
	return filepath.Join(repo, "scripts", "fuzz-smoke.sh")
}

// fuzzTarget is one legal fuzz target. The parameter *name* is carried
// explicitly because that is the whole point of the discovery test below.
type fuzzTarget struct{ name, param string }

// fuzzFile writes a test file declaring each target under one package clause.
func fuzzFile(pkg string, targets ...fuzzTarget) string {
	var b strings.Builder
	b.WriteString("package " + pkg + "\n\nimport \"testing\"\n")
	for _, tgt := range targets {
		b.WriteString("\nfunc " + tgt.name + "(" + tgt.param + " *testing.F) {\n\t" +
			tgt.param + ".Fuzz(func(t *testing.T, b []byte) { _ = b })\n}\n")
	}
	return b.String()
}

// Discovery is the one thing a gate cannot get wrong quietly, so it is pinned
// against the *real* toolchain rather than a stub: a stub can only prove the
// script asks the question, never that the answer it acts on is right.
//
// The fixture carries the two shapes a source grep gets wrong, because both
// were real false greens in an earlier version of this gate:
//
//   - FuzzOddParamName is a legal target whose parameter is not named f. The
//     compiler constrains the parameter *type*, never its name, so a pattern
//     that spells the name finds 1 target in 2 and the gate reports PASS.
//   - FuzzInSubPackage is in a package below the root, which a walk of the
//     root directory alone never reaches.
//
// Nothing here is exotic: both shapes are ordinary Go, and both were written
// by accident before the gate's discovery mechanism was pinned down.
func TestFuzzSmokeFindsEveryTargetTheToolchainCanRun(t *testing.T) {
	script := fixtureRepo(t, map[string]string{
		"pkg/codec/codec_test.go": fuzzFile("codec",
			fuzzTarget{"FuzzRealTarget", "f"},
			fuzzTarget{"FuzzOddParamName", "t"}),
		"pkg/codec/sub/sub_test.go":           fuzzFile("sub", fuzzTarget{"FuzzInSubPackage", "z"}),
		"pkg/codec/testdata/fuzz/FuzzStale/x": "go test fuzz v1\n[]byte{1}\n",
	})

	code, out := runFuzz(t, script, "--list")
	if code != 0 {
		t.Fatalf("--list exit = %d, want 0\n%s", code, out)
	}
	for _, want := range []string{
		"example.com/fixture/pkg/codec\tFuzzRealTarget",
		"example.com/fixture/pkg/codec\tFuzzOddParamName",
		"example.com/fixture/pkg/codec/sub\tFuzzInSubPackage",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("target the toolchain can run was not discovered: %s\n%s",
				strings.ReplaceAll(want, "\t", " "), out)
		}
	}
	// A crash corpus names a target but is not one: the plain `go test` seed
	// run already replays it, and re-deriving a target from it would hand
	// `go test -fuzz` a target that does not exist.
	if strings.Contains(out, "FuzzStale") {
		t.Errorf("a crash corpus was rediscovered as a source target\n%s", out)
	}
}

// A package that does not build cannot be enumerated, and "fewer targets" is
// indistinguishable from "nothing to do" in a summary line -- so the gate has
// to fail rather than shrug.
//
// The trigger is not synthetic: the compiler rejects any Fuzz-named function
// that is not func FuzzX(*testing.F), so a Test-shaped helper that someone
// renamed into the Fuzz prefix breaks its own package. (This is also why the
// discovery test above has no decoy to filter -- such a decoy cannot exist in
// a package that compiles.)
func TestFuzzSmokeFailsLoudlyWhenAPackageDoesNotBuild(t *testing.T) {
	script := fixtureRepo(t, map[string]string{
		"pkg/codec/codec_test.go": "package codec\n\nimport \"testing\"\n\nfunc FuzzNotATarget(t *testing.T) {}\n",
	})

	code, out := runFuzz(t, script)
	if code != 1 {
		t.Fatalf("exit = %d, want 1 when a package cannot be enumerated\n%s", code, out)
	}
	if !strings.Contains(out, "discovery failed") {
		t.Errorf("output does not say that discovery is what failed\n%s", out)
	}
	if strings.Contains(out, "PASS") {
		t.Errorf("a partial target set was reported as a pass\n%s", out)
	}
}

// A repo with no fuzz targets must fail, not pass. "0 targets, 0 crashes" is
// the most dangerous green a gate like this can produce: it survives every
// future commit while checking nothing.
//
// Discovery legitimately invokes `go` -- asking the toolchain *is* the
// mechanism -- so the assertion is not "go was never called" but "no fuzz
// run was started".
func TestFuzzSmokeFailsWhenNoTargetsExist(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "log")
	stubGo(t, logPath, "")
	t.Setenv("FUZZ_STUB_TARGETS", "") // packages exist, targets do not

	code, out := runFuzz(t, fuzzScript(t))
	if code != 1 {
		t.Fatalf("exit = %d, want 1 for a repo with no fuzz targets\n%s", code, out)
	}
	if !strings.Contains(out, "no fuzz targets found") {
		t.Errorf("output does not explain why nothing was checked\n%s", out)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("discovery never consulted the toolchain: %v", err)
	}
	if strings.Contains(string(raw), "-fuzz\n") {
		t.Errorf("a fuzz run was started despite there being no targets\n%s", raw)
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
