#!/usr/bin/env bash
# Performance-mode A/B of baseline (main 30b19dc) vs branch (ack-state-lock).
# Workload matches the 6-node series in docs/optimizations/grpc-io-bottleneck.md
# and replication-batch-linger.md: performance mode, 6 nodes, seed 1, 300k keys,
# RF=3, 2 ms linger, default workers. Both profilers on (phase C had RPC, phase E
# had writes). Rep 0 is a warm-up pair, excluded from analysis.
# Run from the hashring-rs repo root, under caffeinate.
set -u
S="${S:?set S to a directory holding bin/baseline and bin/branch}"
REPS="${REPS:-8}"
OUT="${OUT:-$S/perf-ab-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"
LOG="$OUT/host.log"
CELLS=(OO-64 OO-256 FS-64 FS-256)
flags() {
  case "$1" in
    OO-64)  echo "--policy owner-only --concurrency 64" ;;
    OO-256) echo "--policy owner-only --concurrency 256" ;;
    FS-64)  echo "--policy first-successor --concurrency 64" ;;
    FS-256) echo "--policy first-successor --concurrency 256" ;;
  esac
}
start=$(date +%s)
for rep in $(seq 0 "$REPS"); do
  # Rotate cell order each rep so drift spreads across cells.
  n=${#CELLS[@]}
  for k in $(seq 0 $((n - 1))); do
    cell=${CELLS[$(( (k + rep) % n ))]}
    # Alternate which variant runs first, per rep and cell.
    if (( (rep + k) % 2 )); then order="branch baseline"; else order="baseline branch"; fi
    for v in $order; do
      {
        printf "%s rep=%s cell=%s variant=%s load=%s backupd=%s\n" \
          "$(date '+%H:%M:%S')" "$rep" "$cell" "$v" \
          "$(sysctl -n vm.loadavg)" "$(pgrep -x backupd >/dev/null && echo running || echo idle)"
      } >> "$LOG"
      BIN="$S/bin/$v" PROFILE_WRITES=1 HASHRING_PROFILE_RPC=1 \
        scripts/perf/run.sh "$OUT" "$cell-$v" "$rep" --keys 300000 $(flags "$cell")
    done
  done
done
python3 -I scripts/perf/collect.py "$OUT" > /dev/null
echo "total wall: $(( $(date +%s) - start ))s"
echo "runs: $OUT/runs.tsv"
echo "host log: $LOG"
