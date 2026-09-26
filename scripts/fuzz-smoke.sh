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
# moment someone adds a Fuzz function and forgets the workflow. Discovering
# them from the source means a new target is gated the moment it exists, and a
# repo that somehow has no targets at all fails loudly instead of reporting
# "0 targets, 0 crashes" -- the most dangerous possible green.
#
# Why no -race: the race detector cuts mutation throughput by about an order
# of magnitude, so the same wall-clock budget buys a tenth of the search. The
# division of labour is deliberate: the `go test -race` step owns data races,
# this step owns panics and the invariants asserted inside the targets.
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
	# lines 2-32: the header comment block; anything past it is code.
	sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'
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

# One "dir<TAB>target" line per discovered target.
#
# The source is the authority here: a Fuzz function has no other declaration
# form, so grepping for the signature finds every target the toolchain can
# run. .git and testdata are excluded -- the former holds no Go files worth
# scanning, the latter holds crash corpora that are already covered by the
# plain `go test` seed run.
discover() {
	grep -rEn '^func Fuzz[A-Za-z0-9_]+\(f \*testing\.F\)' \
		--include='*_test.go' --exclude-dir=.git --exclude-dir=testdata "$ROOT" |
		awk -F: '
			{
				# grep -rEn emits "<path>:<line>:<content>", so the
				# signature is field 3, not 2.
				file = $1
				name = $3
				sub(/^func /, "", name)
				sub(/\(f.*$/, "", name)
				dir = file
				sub(/\/[^/]+$/, "", dir)
				print dir "\t" name
			}
		' | sort -u
}

TARGETS="$(discover || true)"

if [ -z "$TARGETS" ]; then
	echo "fuzz-smoke: no fuzz targets found -- refusing to report 0 targets / 0 crashes as a pass" >&2
	exit 1
fi

if [ "$LIST_ONLY" -eq 1 ]; then
	printf '%s\n' "$TARGETS" | while IFS="$(printf '\t')" read -r dir target; do
		printf '%s\t%s\n' "${dir#"$ROOT"/}" "$target"
	done
	exit 0
fi

# Run from the repo root regardless of the caller's cwd so the package
# patterns resolve the same way every time.
cd "$ROOT"

total=0
failed=0
failed_names=""

while IFS="$(printf '\t')" read -r dir target; do
	[ -n "$target" ] || continue
	pkg="./${dir#"$ROOT"/}"
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
