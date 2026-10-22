#!/usr/bin/env bash
# scaling-matrix.sh — the formal matrix of the `scaling` experiment mode.
#
# The groups are the ones in docs/scaling-under-load.md section 9. Each group
# runs on a fresh cluster and writes its own output directory. A run that
# fails is kept; the next repetition still runs, and the script exits non-zero
# at the end if any run failed.
#
# Usage
# -----
#   cargo build --release
#   ./scripts/scaling-matrix.sh
#   REPS=1 ./scripts/scaling-matrix.sh              # smoke the matrix itself
#   KEYS=1000000 ./scripts/scaling-matrix.sh        # the large formal scale
#   GROUPS="main control" ./scripts/scaling-matrix.sh
#   REQUIRE_CLEAN=0 ./scripts/scaling-matrix.sh     # from a dirty tree, not formal
#
# Environment
# -----------
#   BINARY         target/release/hashring-rs
#   OUT_DIR        results/scaling-matrix-<UTC timestamp>
#   KEYS           100000 (the plan's main scale; 1000000 for the large scale)
#   REPS           3
#   GROUPS         main control diagnostic policy open
#   OPEN_RATE      60% of the measured closed-loop capacity, so it must be set
#                  by the operator; the default 6000 is only a starting point
#   OPEN_POOL_SECONDS  20; the 100k key space limits how long a high-rate open
#                  pool can last
#   REQUIRE_CLEAN  1
#
set -euo pipefail

BINARY="${BINARY:-target/release/hashring-rs}"
KEYS="${KEYS:-100000}"
REPS="${REPS:-3}"
GROUPS="${GROUPS:-main control diagnostic policy open}"
OPEN_RATE="${OPEN_RATE:-6000}"
OPEN_POOL_SECONDS="${OPEN_POOL_SECONDS:-20}"
MAX_IN_FLIGHT="${MAX_IN_FLIGHT:-4096}"
REQUIRE_CLEAN="${REQUIRE_CLEAN:-1}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-results/scaling-matrix-$STAMP}"

if [[ ! -x "$BINARY" ]]; then
  echo "error: '$BINARY' not found; run 'cargo build --release' first." >&2
  exit 1
fi
if [[ -e "$OUT_DIR" ]]; then
  echo "error: '$OUT_DIR' already exists; each campaign needs a new OUT_DIR." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
mkdir -p "$OUT_DIR"

{
  echo "binary=$BINARY"
  echo "keys=$KEYS"
  echo "reps=$REPS"
  echo "groups=$GROUPS"
  echo "open_rate=$OPEN_RATE"
  echo "require_clean=$REQUIRE_CLEAN"
  uname -a
  if [[ -r /proc/cpuinfo ]]; then
    grep -m1 'model name' /proc/cpuinfo || true
    echo "cpus=$(grep -c '^processor' /proc/cpuinfo)"
  fi
  if [[ -r /proc/meminfo ]]; then
    grep -m1 'MemTotal' /proc/meminfo || true
  fi
} > "$OUT_DIR/host.txt"

FAILED=0
run() {
  local group="$1" rep="$2"
  shift 2
  local output="$OUT_DIR/$group-$rep"
  echo "running $group repetition $rep -> $output"
  if "$BINARY" experiment --mode scaling --output "$output" --keys "$KEYS" "$@"; then
    echo "$group $rep ok" >> "$OUT_DIR/runs.tsv"
  else
    echo "$group $rep failed" >> "$OUT_DIR/runs.tsv"
    FAILED=1
  fi
}

for group in $GROUPS; do
  for ((rep = 1; rep <= REPS; rep++)); do
    common=(--warmup-seconds 10 --scaling-post-seconds 10)
    if [[ "$REQUIRE_CLEAN" == "1" ]]; then
      common+=(--require-clean-source)
    fi
    case "$group" in
      main)
        run "$group" "$rep" --scaling-scenario round-trip \
          --scaling-pool-seconds 120 "${common[@]}"
        ;;
      control)
        run "$group" "$rep" --scaling-scenario control \
          --scaling-pool-seconds 120 "${common[@]}"
        ;;
      diagnostic)
        # A 250 ms pre-publication delay is a diagnostic, never part of main.
        run "$group" "$rep" --scaling-scenario round-trip \
          --scaling-pool-seconds 120 --pre-publish-delay-ms 250 "${common[@]}"
        ;;
      policy)
        run "$group" "$rep" --scaling-scenario round-trip \
          --scaling-pool-seconds 120 --policy first-successor "${common[@]}"
        ;;
      open)
        # The rate must be 60% of a capacity measured under the same
        # configuration; the footnote belongs in the report.
        run "$group" "$rep" --scaling-scenario round-trip \
          --scaling-load-mode open --scaling-open-rate "$OPEN_RATE" \
          --scaling-pool-seconds "$OPEN_POOL_SECONDS" \
          --max-in-flight "$MAX_IN_FLIGHT" "${common[@]}"
        ;;
      replace)
        run "$group" "$rep" --scaling-scenario replace --nodes 11 \
          "${common[@]}"
        ;;
      churn)
        run "$group" "$rep" --scaling-scenario churn --nodes 9 \
          --initial-nodes 6 "${common[@]}"
        ;;
      *)
        echo "error: unknown group '$group'" >&2
        exit 1
        ;;
    esac
  done
done

echo "runs: $OUT_DIR/runs.tsv"
if ((FAILED)); then
  exit 1
fi
