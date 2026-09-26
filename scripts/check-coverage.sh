#!/usr/bin/env bash
# Enforce the per-package statement-coverage floor this repo advertises.
#
# Why a gate instead of trusting the README: "100% coverage" is a promise
# that only stays true if something fails the build when it stops being
# true. Coverage decays silently -- someone deletes a test, or adds a new
# error branch, and the suite stays green while the claim rots. This script
# is the thing that notices.
#
# The unit of enforcement is the *package*, not the repo total. A repo-wide
# total can hide a package at 60% behind a big package at 100%, and the
# README makes a per-package promise ("每个包的语句覆盖率 100%"), so that
# is exactly what is checked here.
#
# Usage:
#   scripts/check-coverage.sh                        # run tests, then gate
#   scripts/check-coverage.sh --threshold 95         # relaxed floor
#   scripts/check-coverage.sh --profile cov.out      # gate an existing
#                                                    # profile (no tests run)
#
# Exit status: 0 = every package meets the floor, 1 = at least one does not,
# 2 = bad usage. Packages with no statements at all (pure interface
# packages such as pkg/event) are skipped -- there is nothing to cover, and
# counting them would make the ratio undefined.
set -euo pipefail

THRESHOLD="100.0"
PROFILE=""
PKGS="./..."

usage() {
	# lines 2-24: the header comment block; anything past it is code.
	sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
}

die() {
	echo "check-coverage: $*" >&2
	exit 2
}

while [ $# -gt 0 ]; do
	case "$1" in
	--threshold)
		[ $# -ge 2 ] || die "--threshold needs a value"
		THRESHOLD="$2"
		shift 2
		;;
	--threshold=*) THRESHOLD="${1#*=}"; shift ;;
	--profile)
		[ $# -ge 2 ] || die "--profile needs a path"
		PROFILE="$2"
		shift 2
		;;
	--profile=*) PROFILE="${1#*=}"; shift ;;
	--pkgs)
		[ $# -ge 2 ] || die "--pkgs needs a value"
		PKGS="$2"
		shift 2
		;;
	--pkgs=*) PKGS="${1#*=}"; shift ;;
	-h | --help)
		usage
		exit 0
		;;
	*) usage >&2; die "unknown argument: $1" ;;
	esac
done

case "$THRESHOLD" in
"" | *[!0-9.]*) die "threshold must be a number, got: $THRESHOLD" ;;
esac

# Gating an existing profile must not require running the suite: the CI step
# that calls this has already run the tests, and the script's own tests feed
# it synthetic profiles. So profile mode is fully self-contained -- no repo
# checkout assumptions, no cwd assumptions.
if [ -n "$PROFILE" ]; then
	[ -f "$PROFILE" ] || die "profile not found: $PROFILE"
else
	# Run from the repo root regardless of the caller's cwd so "./..."
	# resolves the same way every time.
	ROOT="$(cd "$(dirname "$0")/.." && pwd)"
	PROFILE="$(mktemp)"
	# shellcheck disable=SC2064 # expand $ROOT now, not at trap time
	trap "rm -f '$PROFILE'" EXIT
	cd "$ROOT"
	# -covermode=atomic keeps counts correct under -race; without it, a
	# racy run can undercount and report a phantom gap.
	#
	# -count=1 is not redundant: `go test` caches results, and a warm
	# GOCACHE (CI restores one via setup-go's default `cache: true`) makes
	# this command report "(cached)" and reuse the *previous* profile --
	# a coverage gate that certifies a stale profile. Same hole, same fix
	# as the -count=1 on the race step.
	echo "check-coverage: running tests to build a coverage profile..."
	# Test failures are reported by the caller's own `go test` step; here
	# we only need the profile, so don't let a failing suite mask a
	# coverage verdict. Record it and keep going.
	if ! go test "$PKGS" -count=1 -covermode=atomic -coverprofile="$PROFILE" >/dev/null 2>&1; then
		echo "check-coverage: warning: 'go test' reported failures; gating the profile anyway" >&2
	fi
	[ -s "$PROFILE" ] || die "no coverage profile was produced"
fi

# Per-package aggregation. A profile line is
#   <file>:<line.col,line.col> <numStmt> <count>
# so: strip the trailing :range, drop the last path segment (the .go file
# itself) to get the import path, then sum numStmt split by count>0.
#
# The package list is insertion-sorted in awk because `for (p in arr)` has
# unspecified order, and this script's output is asserted on by its tests --
# nondeterministic ordering would make those assertions lie.
awk -v threshold="$THRESHOLD" '
	NR > 1 {
		file = $1
		sub(/:[0-9].*$/, "", file)
		n = split(file, seg, "/")
		if (n < 2) next
		pkg = ""
		for (i = 1; i < n; i++) pkg = pkg seg[i] "/"
		sub(/\/$/, "", pkg)
		seen[pkg] = 1
		stmts[pkg] += $2
		if ($3 + 0 > 0) cov[pkg] += $2
	}
	END {
		# collect + insertion sort package paths
		m = 0
		for (p in seen) { m++; names[m] = p }
		for (i = 2; i <= m; i++) {
			v = names[i]; j = i - 1
			while (j >= 1 && names[j] > v) { names[j + 1] = names[j]; j-- }
			names[j + 1] = v
		}

		fails = 0; counted = 0; tc = 0; ts = 0
		for (i = 1; i <= m; i++) {
			p = names[i]
			s = stmts[p] + 0; c = cov[p] + 0
			if (s == 0) continue          # pure interface package
			counted++
			pct = 100 * c / s
			printf "%-56s %7d/%-7d %8.2f%%\n", p, c, s, pct
			tc += c; ts += s
			# epsilon guards the boundary: a package at exactly the
			# threshold must pass, not fail on float noise.
			if (pct < threshold - 1e-9) {
				printf "%-56s   ^ below %s%%\n", p, threshold
				fails++
			}
		}

		if (counted == 0) {
			print "check-coverage: no packages with statements found"
			exit 1
		}

		printf "\nTOTAL %d/%d statements (%.2f%%) across %d package(s) with statements\n", \
			tc, ts, 100 * tc / ts, counted

		if (fails > 0) {
			printf "check-coverage: FAIL -- %d of %d package(s) below %s%%\n", \
				fails, counted, threshold
			exit 1
		}
		printf "check-coverage: PASS -- every package is at or above %s%%\n", threshold
	}
' "$PROFILE"
