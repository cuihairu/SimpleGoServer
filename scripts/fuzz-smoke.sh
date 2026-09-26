#!/usr/bin/env bash
# Run every fuzz target in the repo for a fixed per-target budget.
#
# Why a gate: the repo's fuzz targets are the only thing that hunts for input
# no human thought of -- "arbitrary bytes must not panic", "a length field that
# survives validation must stay within bounds", "anything Encode accepts must
# decode back identical". Those invariants rot silently: nobody notices a
# parser regression until a hostile peer finds it. A green `go test` proves
# only that the *seed* corpus is still clean, which is the easy half.
#
# Why discovery instead of a hardcoded list: a target list in CI decays the
# moment someone adds a Fuzz function and forgets the workflow. Asking the
# toolchain (`go test -list`) rather than grepping the sources means a new
# target is gated the moment it exists -- and, just as important, that a
# target which stops being runnable (renamed, moved behind a build tag)
# stops being claimed as covered. A repo that somehow has no targets at all
# fails loudly instead of reporting "0 targets, 0 crashes" -- the most
# dangerous possible green. Why not grep the sources: see the discovery
# function below.
#
# Why no -race: the race detector cuts mutation throughput by about an order
# of magnitude -- measured on this repo's FuzzDecodeStream at a fixed budget,
# the with/without ratio lands around 10x across samples (absolute execs
# counts are not reproducible: on a shared box two identical configurations
# can differ by 20x -- only the ratio is usable), so the same wall clock buys
# a tenth of the search. The division of labour is deliberate: the
# `go test -race` step owns data races, this step owns panics and the
# invariants asserted inside the targets.
#
# A crash is not just a failure -- `go test -fuzz` also writes the offending
# input to testdata/fuzz/<Target>/, so committing that file turns a one-off
# find into a permanent regression case that every later `go test` replays.
#
# Usage:
#   scripts/fuzz-smoke.sh                 # 20s per target (what CI runs)
#   scripts/fuzz-smoke.sh --fuzztime 5m   # a deeper local dig
#   scripts/fuzz-smoke.sh --list          # print the discovered targets
#
# Exit status: 0 = every target survived its budget, 1 = at least one crashed
# (or no targets were found), 2 = bad usage.
set -euo pipefail

FUZZTIME="20s"
LIST_ONLY=0
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

usage() {
	# The header comment block: every leading '#' line after the shebang.
	#
	# Derived rather than spelled as a line range, because a hardcoded line
	# range goes stale the moment a comment is added -- and it fails in the
	# worst way: silently truncating the usage text, or leaking shell source
	# into it. Both happened here already, in this script and in
	# check-coverage.sh. Help text is not the place for rot.
	awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "$0"
}

die() {
	echo "fuzz-smoke: $*" >&2
	exit 2
}

while [ $# -gt 0 ]; do
	case "$1" in
	--fuzztime)
		[ $# -ge 2 ] || die "--fuzztime needs a value"
		FUZZTIME="$2"
		shift 2
		;;
	--fuzztime=*) FUZZTIME="${1#*=}"; shift ;;
	--list) LIST_ONLY=1; shift ;;
	-h | --help)
		usage
		exit 0
		;;
	*) usage >&2; die "unknown argument: $1" ;;
	esac
done

# `-fuzztime` is handed straight to `go test`, so validate the shape here
# rather than letting a typo surface as a confusing flag error mid-run.
#
# The iteration form (`20x`) that `go test` also accepts is rejected on
# purpose: a gate's budget has to mean the same amount of search everywhere,
# and a fixed exec count buys a different budget on every machine.
case "$FUZZTIME" in
*x*) die "fuzztime must be a duration (20s, 2m), not an iteration count: a fixed exec count searches a different amount on every machine" ;;
*[!0-9a-zsmh]*) die "fuzztime must be digits with an optional duration unit, got: $FUZZTIME" ;;
esac
case "$FUZZTIME" in
*[0-9]*) ;;
*) die "fuzztime must contain at least one digit, got: $FUZZTIME" ;;
esac

# One "pkg<TAB>target" line per discovered target.
#
# The toolchain is the authority, not a regex over the sources. Two reasons,
# both measured rather than argued (see scripts/fuzz_smoke_test.go):
#
#  1. Grepping for the signature silently under-counts. The compiler requires
#     any Fuzz-named function to take a *testing.F, but it does not require
#     the parameter to be *named* f -- `func FuzzX(t *testing.F)` compiles and
#     fuzzes fine. A pattern that spells the name finds 1 of 3 legal targets in
#     a three-target fixture, and the gate reports PASS while quietly skipping
#     the rest. That is the exact failure this gate exists to prevent, reached
#     through the mechanism meant to prevent it.
#  2. Source scanning cannot see build tags, so it also over-counts: a target
#     behind `//go:build windows` is found on Linux and then fails to fuzz,
#     turning a red build into a lie ("a target crashed") with no corpus to
#     commit.
#
# `go test -list` cannot over-report either: in a package that compiles, every
# Fuzz-named function *is* a runnable target, because the signature check
# above rejects anything else at build time. So for a compiling package the
# toolchain's answer is exact in both directions -- which is also why there is
# no decoy filtering to do here.
# Every package pattern below is relative, so the gate runs from the repo root
# regardless of the caller's cwd -- and it has to do that *before* discovery,
# not after. `go test` runs a test binary with the working directory set to
# the package under test, so a gate invoked from a test would otherwise ask
# `go list` about the test's own directory and enumerate the wrong tree. (A
# real gate bug, caught by the fixture test that runs this script from
# elsewhere; the previous source-scanning version was accidentally immune
# because it walked absolute paths.)
cd "$ROOT"

discover() {
	local pkg names
	# `go list` rather than a file walk: it is the build's own view of which
	# packages exist for this platform.
	for pkg in $(go list ./...); do
		# Deliberately not `|| true`: a package that fails to build must
		# fail the gate loudly, because skipping it would drop its targets
		# from the run without a word. `go test -list` still exits 0 for a
		# package with no test files, so this cannot fire spuriously.
		names="$(go test -list 'Fuzz.*' "$pkg")" || return 1
		local name
		for name in $(printf '%s\n' "$names" | grep '^Fuzz' || true); do
			printf '%s\t%s\n' "$pkg" "$name"
		done
	done
}

if ! TARGETS="$(discover)"; then
	echo "fuzz-smoke: discovery failed -- a package would not build, so its targets" >&2
	echo "fuzz-smoke: cannot be enumerated; refusing to fuzz a partial target set" >&2
	exit 1
fi

if [ -z "$TARGETS" ]; then
	echo "fuzz-smoke: no fuzz targets found -- refusing to report 0 targets / 0 crashes as a pass" >&2
	exit 1
fi

if [ "$LIST_ONLY" -eq 1 ]; then
	printf '%s\n' "$TARGETS" | while IFS="$(printf '\t')" read -r pkg target; do
		printf '%s\t%s\n' "$pkg" "$target"
	done
	exit 0
fi

total=0
failed=0
failed_names=""

while IFS="$(printf '\t')" read -r pkg target; do
	[ -n "$target" ] || continue
	total=$((total + 1))
	printf 'fuzz-smoke: %s %s (%s)\n' "$target" "$FUZZTIME" "$pkg"
	# A crash prints its own Go-level failure report; only the verdict is
	# summarised here, so the log stays readable with many targets.
	if go test "$pkg" -run '^$' -fuzz "^${target}\$" -fuzztime "$FUZZTIME"; then
		printf 'fuzz-smoke:   %s survived %s\n' "$target" "$FUZZTIME"
	else
		printf 'fuzz-smoke:   %s CRASHED -- crash input written to testdata/fuzz/%s/, commit it\n' \
			"$target" "$target" >&2
		failed=$((failed + 1))
		failed_names="${failed_names} ${target}"
	fi
done <<<"$TARGETS"

if [ "$failed" -gt 0 ]; then
	echo "fuzz-smoke: FAIL -- ${failed} of ${total} target(s) crashed:${failed_names}" >&2
	exit 1
fi

echo "fuzz-smoke: PASS -- ${total} target(s), ${FUZZTIME} each"
