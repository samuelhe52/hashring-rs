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
#   RUN_GROUPS="main control" ./scripts/scaling-matrix.sh
#   REQUIRE_CLEAN=0 ./scripts/scaling-matrix.sh     # from a dirty tree, not formal
#
# Environment
# -----------
#   BINARY         target/release/hashring-rs
#   OUT_DIR        results/scaling-matrix-<UTC timestamp>
#   KEYS           100000 (the plan's main scale; 1000000 for the large scale)
#   REPS           3
#   RUN_GROUPS     main control diagnostic policy replace churn open
#   OPEN_RATE      50000; the rate must be 60% of a capacity measured under
#                  the same configuration
#   OPEN_POOL_SECONDS  33
#   OPEN_KEYS      1000000; the open group is recorded at 1M keys
#   OPEN_MAX_DEDUP_BYTES  268435456; the open group's high write rate needs
#                  room for receipts on top of a large preload
#   OPEN_POST_SECONDS  5; the open group's post window, so its pools last the
#                  whole transition
#   REQUIRE_CLEAN  1
#
set -euo pipefail

BINARY="${BINARY:-target/release/hashring-rs}"
KEYS="${KEYS:-100000}"
REPS="${REPS:-3}"
RUN_GROUPS="${RUN_GROUPS:-main control diagnostic policy replace churn open}"
OPEN_RATE="${OPEN_RATE:-50000}"
OPEN_POOL_SECONDS="${OPEN_POOL_SECONDS:-33}"
# The open group is the only one whose write rate needs the receipt budget:
# a 1M-key preload holds roughly 95 MB per node, and the open-loop writes add
# several MB/s on top of that.
OPEN_MAX_DEDUP_BYTES="${OPEN_MAX_DEDUP_BYTES:-268435456}"
# The open group's transition already holds the load for its warmup and its
# RF convergence, and its one-shot pools must survive the whole transition.
OPEN_POST_SECONDS="${OPEN_POST_SECONDS:-5}"
# The open group is recorded at 1M keys in section 9; the closed groups use
# KEYS.
OPEN_KEYS="${OPEN_KEYS:-1000000}"
MAX_IN_FLIGHT="${MAX_IN_FLIGHT:-4096}"
REQUIRE_CLEAN="${REQUIRE_CLEAN:-1}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-results/scaling-matrix-$STAMP}"

# Both guards below test paths that are relative to the project root, so the
# script must be there before it checks them. Otherwise a run from another
# directory reports a missing binary that is present, and the OUT_DIR guard
# protects a directory other than the one the campaign writes to.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

if [[ ! -x "$BINARY" ]]; then
  echo "error: '$BINARY' not found; run 'cargo build --release' first." >&2
  exit 1
fi
if [[ -e "$OUT_DIR" ]]; then
  echo "error: '$OUT_DIR' already exists; each campaign needs a new OUT_DIR." >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

{
  echo "binary=$BINARY"
  echo "keys=$KEYS"
  echo "reps=$REPS"
  echo "groups=$RUN_GROUPS"
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

for group in $RUN_GROUPS; do
  for ((rep = 1; rep <= REPS; rep++)); do
    common=(--warmup-seconds 10)
    post=(--scaling-post-seconds 10)
    if [[ "$REQUIRE_CLEAN" == "1" ]]; then
      common+=(--require-clean-source)
    fi
    case "$group" in
      main)
        run "$group" "$rep" --scaling-scenario round-trip \
          --scaling-pool-seconds 120 "${common[@]}" "${post[@]}"
        ;;
      control)
        run "$group" "$rep" --scaling-scenario control \
          --scaling-pool-seconds 120 "${common[@]}" "${post[@]}"
        ;;
      diagnostic)
        # A 250 ms pre-publication delay is a diagnostic, never part of main.
        run "$group" "$rep" --scaling-scenario round-trip \
          --scaling-pool-seconds 120 --pre-publish-delay-ms 250 "${common[@]}" "${post[@]}"
        ;;
      policy)
        run "$group" "$rep" --scaling-scenario round-trip \
          --scaling-pool-seconds 120 --policy first-successor "${common[@]}" "${post[@]}"
        ;;
      open)
        # The rate must be 60% of a capacity measured under the same
        # configuration; section 9 records it at 1M keys, 50k ops/s, and a
        # 33-second pool.
        post=(--scaling-post-seconds "$OPEN_POST_SECONDS")
        run "$group" "$rep" --scaling-scenario round-trip \
          --scaling-load-mode open --scaling-open-rate "$OPEN_RATE" \
          --scaling-pool-seconds "$OPEN_POOL_SECONDS" \
          --keys "$OPEN_KEYS" \
          --max-in-flight "$MAX_IN_FLIGHT" \
          --max-dedup-bytes "$OPEN_MAX_DEDUP_BYTES" "${common[@]}" "${post[@]}"
        ;;
      replace)
        run "$group" "$rep" --scaling-scenario replace --nodes 11 \
          "${common[@]}" "${post[@]}"
        ;;
      churn)
        run "$group" "$rep" --scaling-scenario churn --nodes 9 \
          --initial-nodes 6 "${common[@]}" "${post[@]}"
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
