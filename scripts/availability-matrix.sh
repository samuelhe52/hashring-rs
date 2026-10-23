#!/usr/bin/env bash
# availability-matrix.sh — the campaign for the stage 4 consecutive failures.
#
# Every run injects the same number of consecutive owner failures. Each round
# kills the current owner of the probe key, waits for the automatic failover,
# and then joins a replacement and waits for the full replication factor
# again. The groups differ in the number of virtual nodes, which is what
# decides how evenly the remaining members hold the keys.
#
# Usage
# -----
#   cargo build --release
#   ./scripts/availability-matrix.sh
#   REPS=1 ./scripts/availability-matrix.sh              # smoke the campaign
#   KEYS=10000 ./scripts/availability-matrix.sh          # a smaller key space
#   GROUPS="v1024" ./scripts/availability-matrix.sh      # one group only
#   REQUIRE_CLEAN=0 ./scripts/availability-matrix.sh     # from a dirty tree
#
# Environment
# -----------
#   BINARY         target/release/hashring-rs
#   OUT_DIR        results/availability-matrix-<UTC timestamp>
#   ROUNDS         3 consecutive failures per run
#   NODES          9: 6 initial members and 3 replacements
#   KEYS           100000
#   REPS           3
#   GROUPS         v128 v512 v1024
#   REQUIRE_CLEAN  1

set -euo pipefail

BINARY="${BINARY:-target/release/hashring-rs}"
ROUNDS="${ROUNDS:-3}"
NODES="${NODES:-9}"
KEYS="${KEYS:-100000}"
REPS="${REPS:-3}"
GROUPS="${GROUPS:-v128 v512 v1024}"
REQUIRE_CLEAN="${REQUIRE_CLEAN:-1}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-results/availability-matrix-$STAMP}"

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
  echo "rounds=$ROUNDS"
  echo "nodes=$NODES"
  echo "keys=$KEYS"
  echo "reps=$REPS"
  echo "groups=$GROUPS"
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
  local group="$1" rep="$2" vnodes="$3"
  local output="$OUT_DIR/$group-$rep"
  echo "running $group repetition $rep (virtual nodes $vnodes) -> $output"
  local arguments=(
    experiment --mode availability
    --availability-rounds "$ROUNDS"
    --nodes "$NODES"
    --keys "$KEYS"
    --virtual-nodes "$vnodes"
    --policy first-successor
    --output "$output"
  )
  if [[ "$REQUIRE_CLEAN" == "1" ]]; then
    arguments+=(--require-clean-source)
  fi
  if "$BINARY" "${arguments[@]}"; then
    echo "$group $rep ok" >> "$OUT_DIR/runs.tsv"
  else
    echo "$group $rep failed" >> "$OUT_DIR/runs.tsv"
    FAILED=1
  fi
}

for group in $GROUPS; do
  case "$group" in
    v128) vnodes=128 ;;
    v256) vnodes=256 ;;
    v512) vnodes=512 ;;
    v1024) vnodes=1024 ;;
    *)
      echo "error: unknown group '$group'" >&2
      exit 1
      ;;
  esac
  for ((rep = 1; rep <= REPS; rep++)); do
    run "$group" "$rep" "$vnodes"
  done
done

echo "runs: $OUT_DIR/runs.tsv"
if ((FAILED)); then
  exit 1
fi
