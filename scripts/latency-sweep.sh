#!/usr/bin/env bash
# latency-sweep.sh — the acceptance run of the `latency` experiment mode.
#
# What it does
# ------------
#   1. Measures the capacity of the cluster with `performance` mode, which is
#      the closed-loop write throughput.
#   2. Runs the `latency` mode at a series of target rates, for every key
#      distribution, and repeats the whole thing, so every number has an
#      error bar.
#   3. Runs the same load at a series of virtual node counts, to see how
#      evenly the ring divides the key space.
#
# Why the rates are a share of the capacity
# -----------------------------------------
#   The target rate is the input of a run, not its output. A rate above what
#      the cluster can do does not measure the cluster, it measures the
#      driver, so the rates are a share of what `performance` mode measured.
#      The shares are 0.25, 0.5, 0.75 and 0.9, and the ladder afterwards
#      shows where the mode actually gives up.
#
# The repetitions are interleaved
# -------------------------------
#   The distributions are run in the order uniform, zipf, uniform, zipf, ...
#      so a machine that drifts over the half hour of the campaign moves both
#      groups in the same direction, instead of moving one group and then the
#      other.
#
# Usage
# -----
#   ./scripts/latency-sweep.sh
#   REPS=5 ./scripts/latency-sweep.sh
#   RATES="5000 10000 20000" ./scripts/latency-sweep.sh     # skip the capacity run
#   DISTRIBUTIONS="uniform" ./scripts/latency-sweep.sh
#   LADDER=0 ./scripts/latency-sweep.sh
#   SKIP_CAMPAIGN=1 ./scripts/latency-sweep.sh               # only the ladder and the rings
#   FIRST_REP=3 REPS=5 ./scripts/latency-sweep.sh            # start the repetitions at three
#   SKIP_VNODES=1 ./scripts/latency-sweep.sh                  # no virtual node comparison
#   OUT_DIR=results/latency-acceptance-20261022-002702 ./scripts/latency-sweep.sh
#
# Output
# ------
#   results/latency-acceptance-<timestamp>/   the experiment output
#   docs/data/latency-acceptance/*.tsv       one row per stage, and the
#                                             medians over the repetitions

set -euo pipefail

BINARY="${BINARY:-target/release/hashring-rs}"
NODES="${NODES:-10}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
POLICY="${POLICY:-owner-only}"
VALUE_BYTES="${VALUE_BYTES:-128}"
PRELOAD_KEYS="${PRELOAD_KEYS:-100000}"
MIX="${MIX:-90:9:1}"
WARMUP="${WARMUP:-10}"
DURATION="${DURATION:-60}"
REPS="${REPS:-3}"
FIRST_REP="${FIRST_REP:-1}"
DISTRIBUTIONS="${DISTRIBUTIONS:-uniform zipf:0.99}"
VNODES="${VNODES:-16 64 128 256}"
VNODE_RATES="${VNODE_RATES:-}"
LADDER="${LADDER:-1}"
LADDER_WARMUP="${LADDER_WARMUP:-3}"
LADDER_DURATION="${LADDER_DURATION:-10}"
LADDER_RATES="${LADDER_RATES:-}"
DATA_DIR="${DATA_DIR:-docs/data/latency-acceptance}"
OPERATION_TIMEOUT_MS="${OPERATION_TIMEOUT_MS:-30000}"
MIGRATION_TIMEOUT_MS="${MIGRATION_TIMEOUT_MS:-600000}"

if [[ ! -x "$BINARY" ]]; then
  echo "error: '$BINARY' not found; run 'cargo build --release' first." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

EXTRACT="${EXTRACT:-$PROJECT_ROOT/docs/data/latency-acceptance/extract.py}"

OUT_DIR="${OUT_DIR:-results/latency-acceptance-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT_DIR"
DATA_OUT="$DATA_DIR/$(basename "$OUT_DIR")"

# The host the numbers were measured on. A latency number without the host it
# came from is not a measurement. It is written into the run's own output
# directory, which is under `results/` and out of git, and copied into the
# data directory after the last run: a file in the tracked data directory
# before the first run would be untracked source that every run records in
# its manifest, and every manifest of the campaign would then report
# `source_reproducible: false` for the script's own output.
{
  echo "date: $(date -Is)"
  echo "uname: $(uname -a)"
  echo "cpus: $(nproc)"
  echo "memory_total_kb: $(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  echo "memory_available_kb: $(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
  echo "load: $(cat /proc/loadavg)"
  echo "binary_sha256: $(sha256sum "$BINARY" | cut -d' ' -f1)"
  echo "git_commit: $(git rev-parse --short HEAD)"
  echo "git_changed_files: $(git status --porcelain | wc -l)"
  echo "cargo: $(cargo --version)"
  echo "rustc: $(rustc --version)"
} > "$OUT_DIR/host.txt"
cat "$OUT_DIR/host.txt"

run_experiment() {
  local out="$1"
  shift
  rm -rf "$out"
  echo "  -> $out"
  "$BINARY" experiment --output "$out" "$@" > "$out.log" 2>&1 || {
    echo "  FAILED, see $out.log" >&2
    tail -20 "$out.log" >&2
    return 1
  }
}

# The flag takes a list, and the list is kept space separated here.
as_list() {
  echo "${1// /,}"
}

# The capacity of the cluster, and the rates that follow from it.
capacity=0
if [[ -z "${RATES:-}" ]]; then
  echo "== capacity, performance mode"
  run_experiment "$OUT_DIR/capacity" \
    --mode performance \
    --nodes "$NODES" \
    --desired-replication-factor "$REPLICATION_FACTOR" \
    --policy "$POLICY" \
    --value-bytes "$VALUE_BYTES" \
    --keys "$PRELOAD_KEYS" \
    --operation-timeout-ms "$OPERATION_TIMEOUT_MS" \
    --migration-timeout-ms "$MIGRATION_TIMEOUT_MS"
  capacity="$(python3 - "$OUT_DIR/capacity/summary.json" << 'PY'
import json, sys
summary = json.load(open(sys.argv[1]))
print(int(summary["measurements"]["initial_put"]["operations_per_second"]))
PY
)"
  echo "  capacity: $capacity ops/s"
  RATES="$(python3 - "$capacity" << 'PY'
import sys
capacity = int(sys.argv[1])
print(" ".join(str(int(capacity * share)) for share in (0.25, 0.5, 0.75, 0.9)))
PY
)"
fi
echo "  rates: $RATES"

# The campaign itself.
if [[ "${SKIP_CAMPAIGN:-0}" == "1" ]]; then
  echo "  skipped"
else
  for ((rep = FIRST_REP; rep <= REPS; rep++)); do
    for distribution in $DISTRIBUTIONS; do
      group="${distribution//[:.]/-}"
      echo "== rep $rep, $distribution"
      run_experiment "$OUT_DIR/$group-rep$rep" \
        --mode latency \
        --nodes "$NODES" \
        --desired-replication-factor "$REPLICATION_FACTOR" \
        --policy "$POLICY" \
        --value-bytes "$VALUE_BYTES" \
        --preload-keys "$PRELOAD_KEYS" \
        --target-rates "$(as_list "$RATES")" \
        --warmup-seconds "$WARMUP" \
        --duration-seconds "$DURATION" \
        --mix "$MIX" \
        --key-distribution "$distribution" \
        --virtual-nodes 128 \
        --operation-timeout-ms "$OPERATION_TIMEOUT_MS" \
        --migration-timeout-ms "$MIGRATION_TIMEOUT_MS"
    done
  done
fi
# Where the mode gives up. The capacity run measures the write path, and the
# campaign's mix is mostly reads, so the knee of the curve is somewhere the
# capacity run cannot see.
if [[ "$LADDER" == "1" ]]; then
  if [[ -z "$LADDER_RATES" ]]; then
    LADDER_RATES="$(python3 - "$capacity" << 'PY'
import sys
capacity = int(sys.argv[1]) if sys.argv[1] else 20000
print(" ".join(str(int(capacity * share)) for share in (1.0, 1.5, 2.0, 3.0)))
PY
)"
  fi
  echo "== ladder, $LADDER_RATES"
  run_experiment "$OUT_DIR/ladder" \
    --mode latency \
    --nodes "$NODES" \
    --desired-replication-factor "$REPLICATION_FACTOR" \
    --policy "$POLICY" \
    --value-bytes "$VALUE_BYTES" \
    --preload-keys "$PRELOAD_KEYS" \
    --target-rates "$(as_list "$LADDER_RATES")" \
    --warmup-seconds "$LADDER_WARMUP" \
    --duration-seconds "$LADDER_DURATION" \
    --mix "$MIX" \
    --key-distribution uniform \
    --virtual-nodes 128 \
    --operation-timeout-ms "$OPERATION_TIMEOUT_MS" \
    --migration-timeout-ms "$MIGRATION_TIMEOUT_MS"
fi

# The virtual node comparison. The key space is what the ring divides, so the
# rates only have to be high enough to show the request distribution too.
if [[ -z "$VNODE_RATES" ]]; then
  VNODE_RATES="$(python3 - "$RATES" << 'PY'
import sys
rates = [int(rate) for rate in sys.argv[1].split()]
print(" ".join(str(rate) for rate in (rates[0], rates[len(rates) // 2])))
PY
)"
fi
for vnodes in $VNODES; do
  if [[ "${SKIP_VNODES:-0}" == "1" ]]; then
    break
  fi
  echo "== virtual nodes $vnodes"
  run_experiment "$OUT_DIR/vnodes-$vnodes" \
    --mode latency \
    --nodes "$NODES" \
    --desired-replication-factor "$REPLICATION_FACTOR" \
    --policy "$POLICY" \
    --value-bytes "$VALUE_BYTES" \
    --preload-keys "$PRELOAD_KEYS" \
    --target-rates "$(as_list "$VNODE_RATES")" \
    --warmup-seconds "$WARMUP" \
    --duration-seconds "$DURATION" \
    --mix "$MIX" \
    --key-distribution uniform \
    --virtual-nodes "$vnodes" \
    --operation-timeout-ms "$OPERATION_TIMEOUT_MS" \
    --migration-timeout-ms "$MIGRATION_TIMEOUT_MS"
done

echo "== extracting"
# The data directory is written now that every run is over, so no run saw it.
mkdir -p "$DATA_OUT"
cp "$OUT_DIR/host.txt" "$DATA_OUT/host.txt"
python3 "$EXTRACT" "$OUT_DIR" > "$DATA_OUT/stages.tsv"
python3 "$EXTRACT" --medians "$OUT_DIR" > "$DATA_OUT/medians.tsv"
cp "$DATA_OUT/host.txt" "$DATA_DIR/host.txt"
echo "  $DATA_OUT/stages.tsv"
echo "  $DATA_OUT/medians.tsv"
echo "done"
