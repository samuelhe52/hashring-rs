#!/usr/bin/env bash
# sweep.sh — measure how initial-put throughput responds to offered concurrency.
#
# A single run of this benchmark is not a measurement. Repeated initial-put
# runs of the same binary varied by 41.7% (0.362s to 0.551s at concurrency 128),
# which is wider than most effects worth chasing here. Every point is therefore
# a median over REPS runs, and the counters are read from the same
# node_pressure_sampled event that HASHRING_PROFILE_WRITES populates.
#
# Usage
# -----
#   ./scripts/sweep.sh                    # default concurrencies
#   REPS=5 ./scripts/sweep.sh             # more repetitions
#   CONCS="32 64 128 256" ./scripts/sweep.sh
#
# Output
# ------
#   results/sweep-<timestamp>/<conc>-<rep>/  per-run experiment output
#   results/sweep-<timestamp>/sweep.tsv      one row per run, then medians

set -euo pipefail

REPS="${REPS:-3}"
CONCS="${CONCS:-16 64 128 256 512}"
BINARY="${BINARY:-target/release/hashring-rs}"

if [[ ! -x "$BINARY" ]]; then
  echo "error: '$BINARY' not found; run 'cargo build --release' first." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

OUT_DIR="results/sweep-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT_DIR"

# Read one run's initial put seconds plus the pressure counters that explain it.
read_run() {
  python3 - "$1" <<'PY'
import json, os, sys
d = sys.argv[1]
put_s = None
nodes = {}
for line in open(os.path.join(d, 'events.jsonl')):
    e = json.loads(line)
    if e['event'] == 'initial_put_complete':
        put_s = e['fields']['seconds']
    elif e['event'] == 'node_pressure_sampled':
        nodes = e['fields']['nodes']

def tot(key):
    return sum((n.get(key) or 0) for n in nodes.values())

def timing(path):
    acq = sum(((n.get(path) or {}).get('acquisitions') or 0) for n in nodes.values())
    hold = sum(((n.get(path) or {}).get('hold_nanos') or 0) for n in nodes.values())
    wait = sum(((n.get(path) or {}).get('wait_nanos') or 0) for n in nodes.values())
    return acq, hold, wait

o_acq, o_hold, o_wait = timing('owner_write_timing')
f_acq, f_hold, f_wait = timing('follower_write_timing')
peak = max((n.get('replication_stream_peak_pending') or 0) for n in nodes.values()) if nodes else 0

print('\t'.join(str(x) for x in [
    put_s,
    tot('replication_reservation_rejections'),
    tot('replication_rpc_retries'),
    tot('owner_dedup_rejections'),
    peak,
    o_acq, o_hold, o_wait,
    f_acq, f_hold, f_wait,
]))
PY
}

printf "conc\trep\tput_s\tresv_rej\trpc_retry\tdedup_rej\tpeak_q\towner_acq\towner_hold_ns\towner_wait_ns\tfoll_acq\tfoll_hold_ns\tfoll_wait_ns\n" > "$OUT_DIR/sweep.tsv"

for c in $CONCS; do
  for r in $(seq 1 "$REPS"); do
    run_dir="$OUT_DIR/$c-$r"
    HASHRING_PROFILE_WRITES=1 "$BINARY" experiment \
      --mode correctness --concurrency "$c" --output "$run_dir" >/dev/null 2>&1 || true
    if [[ -f "$run_dir/events.jsonl" ]]; then
      row=$(read_run "$run_dir")
      printf "%s\t%s\t%s\n" "$c" "$r" "$row" >> "$OUT_DIR/sweep.tsv"
      printf "  conc=%-4s rep=%s  put=%ss\n" "$c" "$r" "$(echo "$row" | cut -f1)"
    else
      printf "  conc=%-4s rep=%s  FAILED\n" "$c" "$r"
    fi
    rm -rf "$run_dir"
  done
done

echo
echo "================ MEDIANS ================"
python3 - "$OUT_DIR/sweep.tsv" <<'PY'
import sys, statistics, collections
rows = collections.defaultdict(list)
hdr = None
for line in open(sys.argv[1]):
    parts = line.rstrip('\n').split('\t')
    if parts[0] == 'conc':
        hdr = parts; continue
    rows[parts[0]].append(parts)
if hdr is None:
    raise SystemExit('no rows')
i = {name: k for k, name in enumerate(hdr)}
print(f"{'conc':>5} {'put_s':>7} {'keys/s':>8} {'resv_rej':>9} {'retry':>6} {'dedup':>6} "
      f"{'peak_q':>7} {'own_hold_us':>11} {'own_wait_us':>11} {'own_wait%':>10}")
for c, rs in rows.items():
    def med(name):
        vals = [float(r[i[name]]) for r in rs if r[i[name]] not in ('', 'None')]
        return statistics.median(vals) if vals else float('nan')
    put = med('put_s'); kps = 20000/put if put == put and put else 0
    oh = med('owner_hold_ns')/1000.0; ow = med('owner_wait_ns')/1000.0
    duty = ow/(ow+oh)*100 if (ow+oh) else 0
    print(f"{c:>5} {put:>7.3f} {kps:>8.0f} {med('resv_rej'):>9.0f} {med('rpc_retry'):>6.0f} "
          f"{med('dedup_rej'):>6.0f} {med('peak_q'):>7.0f} {oh:>11.1f} {ow:>11.1f} {duty:>9.0f}%")
print()
print("own_hold/own_wait are cumulative across all nodes, microseconds.")
PY
echo
echo "raw rows: $OUT_DIR/sweep.tsv"
