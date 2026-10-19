#!/usr/bin/env bash
# netem-sweep.sh — measure how the write path responds to injected network RTT.
#
# What it measures
# ----------------
#   One question: is the RPC round trip the limiter on the write path, or is
#   something else? The CPU profiles in results/flamegraph-* show the transport
#   and kernel path is the largest CPU consumer, but
#   docs/optimizations/write-path-saturation.md shows the same runs are
#   latency-bound with spare CPU, and the load generator shares the host. None of
#   that separates "the network is expensive" from "the network is the
#   bottleneck". This script settles it by making the network worse on purpose
#   and plotting throughput and latency against round-trip time.
#
#   Each point reports put throughput, put latency percentiles, whole-run CPU
#   seconds as medians over REPS runs, because an unchanged binary varies by
#   tens of percent run to run (see sweep.sh), and netem's packet drop count as
#   the maximum over them, because one lossy run is enough to disqualify a point.
#
# How the delay is applied
# ------------------------
#   Every process talks over 127.0.0.1, so one qdisc on `lo` covers the driver,
#   the coordinator and all data nodes without touching the binary or the
#   config. netem delays each packet on egress and a loopback packet egresses
#   `lo` once per direction, so RTT is *twice* the configured delay. Measured on
#   this host rather than assumed:
#
#       delay 0 -> RTT 0.037 ms      delay 5ms -> RTT 10.036 ms
#
#   The sweep pings at every point and records the RTT it actually got, so the
#   table is in measured RTT, not in configured delay.
#
# Privilege
# ---------
#   `tc qdisc add` needs CAP_NET_ADMIN, which a normal login lacks, and sudo
#   asks for a password on this host. So the script re-executes itself inside
#   `unshare --user --map-root-user --net`, where the caller is root over a
#   fresh network namespace and netem installs with no privilege escalation.
#   Everything is inherited: the driver, the coordinator and every node run
#   inside that namespace, so all cluster traffic is shaped and nothing outside
#   it is. One side effect worth knowing: the namespace has its own empty
#   netfilter tables, so this host's nftables and conntrack rules leave the
#   path. Throughput measured within one sweep is unaffected, but a profile
#   taken here is not comparable to results/flamegraph-*, which include them.
#
#   Run as root, or with CAP_NET_ADMIN, and UNSHARE=1 to skip the namespace. Then
#   the script shapes this host's loopback, and the qdisc is removed on exit.
#
# Controls
# --------
#   DELAYS="off" removes the qdisc entirely and is the baseline. DELAYS="0" keeps
#   netem attached with no delay and controls for netem's own cost: a shaped
#   loopback queues packets in softirq where an unshaped one does not, so the
#   gap between "off" and "0" is overhead of the instrument, not of an emulated
#   network. Fit the slope from "0" onward, never from "off".
#
#   A point that dropped packets is not comparable: netem dropping means TCP
#   retransmitting, which measures a loss rate rather than a delay. The table
#   prints the drop count and the analysis excludes such points from the fit.
#
# Reading the latency column
# --------------------------
#   `client_put_latency_us` comes from the experiment's own sample of 128 puts
#   issued one at a time right after the initial verify, on an otherwise quiet
#   cluster (crates/hashring-experiment/src/lib.rs:613). It is therefore a
#   *single-op, unloaded* latency, not the latency under --concurrency. That
#   makes its slope against RTT the cleanest quantity here: it is the number of
#   round trips the write path serializes on, with no queueing folded in. It
#   also means the in-flight column, which multiplies throughput by this p50, is
#   an estimate under Little's law with the wrong latency, not a measurement.
#
# Usage
# -----
#   ./scripts/netem-sweep.sh                        # the default sweep
#   DELAYS="off 0 400us 1ms" REPS=5 ./scripts/netem-sweep.sh
#   CONCS="64 256" ./scripts/netem-sweep.sh         # offered load, too
#   RATES="1gbit 100mbit" DELAYS="0 1ms" ./scripts/netem-sweep.sh
#   KEYS=2000 DELAYS="off 1ms" REPS=1 OUT_DIR=/tmp/x ./scripts/netem-sweep.sh  # smoke
#   UNSHARE=1 ./scripts/netem-sweep.sh              # already CAP_NET_ADMIN
#
#   env CONFIG=experiments/experiment_cmach.yaml ./scripts/netem-sweep.sh
#
#   NODE_COUNTS="4 8 12 24" DELAYS="0 400us 1ms" REPS=2 ./scripts/netem-sweep.sh
#
# Settings
# --------
#   The experiment runs with --config CONFIG plus --mode correctness and
#   --nodes/--keys/--concurrency, and a flag wins over the file, so the
#   replication factor, acknowledgement policy, virtual nodes, seed and byte
#   budgets stay the ones of that config and only the run size changes. That keeps the sweep on the same code path as the
#   profiles in results/.
#
#   worker_threads defaults to CPUs / (nodes + 2), so it moves with the node
#   count unless the config pins it. Every row records what the run actually
#   resolved to, because a node-count sweep where the thread count silently
#   changed with it would measure two variables at once and neither.
#
# Output
# ------
#   results/netem-<timestamp>/
#     netem.tsv       one row per run: point, nodes, latency, throughput, CPU,
#                     the worker_threads the run resolved to, and drops
#     medians.tsv     one row per point, medians over the repetitions
#     analysis.txt    latency-vs-RTT slope per node count, the concurrency
#                     ceiling ratio, the crossover RTT, and the drop verdict
#     logs/           per-run experiment output, timing and netem state
#
#   The two derived columns answer different questions and both are printed:
#   ceil_ratio is concurrency / p50 against measured throughput, which says
#   whether *this* point is network-limited, and crossover_RTT_us is the RTT at
#   which the network ceiling would fall to the loopback ceiling, which says
#   whether a realistic link would become the limiter. They disagree on purpose:
#   a configuration can be un-network-limited over the whole swept range and
#   still have a crossover a real LAN would sit above.
#
# What this cannot answer
# -----------------------
#   It shapes one shared loopback link, so every hop is delayed and no bandwidth
#   is charged per hop. A real cluster has a hop per node, per-core link limits,
#   NIC offload, and a receive cost on another machine. This is a test of RTT
#   sensitivity, not a model of a cluster's network.

set -euo pipefail

# "off" removes the qdisc; anything else is a netem delay, and RTT is twice it.
DELAYS="${DELAYS:-off 0 100us 200us 400us}"
# Optional rate limits, crossed with DELAYS. Empty means unlimited bandwidth.
RATES="${RATES:-}"
REPS="${REPS:-3}"
CONCS="${CONCS:-64}"
# Peak node count. A list, because the crossover this script exists to locate is
# not a property of the network: it moves down as the aggregate write ceiling
# rises with node count. medians.tsv records the worker_threads each run actually
# resolved to, so a config that derives it from the node count cannot silently
# confound this axis.
NODE_COUNTS="${NODE_COUNTS:-4}"
KEYS="${KEYS:-100000}"
CONFIG="${CONFIG:-examples/experiment.yaml}"
BINARY="${BINARY:-target/release/hashring-rs}"
# netem's queue length in packets. A run holds far less than this at these
# delays; a full queue drops, which is a different measurement.
LIMIT="${LIMIT:-100000}"
PING_COUNT="${PING_COUNT:-10}"
KEEP_RUNS="${KEEP_RUNS:-0}"
OUT_DIR="${OUT_DIR:-results/netem-$(date +%Y%m%d-%H%M%S)}"

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
PROJECT_ROOT="$(cd "$(dirname "$SCRIPT_PATH")/.." && pwd)"

# Re-exec into the namespace, once. The guard is an environment variable so the
# second pass does not recurse.
if [[ "${NETEM_SWEEP_NS:-0}" != "1" ]]; then
  export NETEM_SWEEP_NS=1
  if [[ "${UNSHARE:-0}" == "1" ]]; then
    echo "==> UNSHARE=1: shaping this host's loopback; needs CAP_NET_ADMIN"
  elif [[ "$(id -u)" -eq 0 ]]; then
    echo "==> re-executing with: unshare --net"
    exec unshare --net "$SCRIPT_PATH" "$@"
  else
    echo "==> re-executing with: unshare --user --map-root-user --net"
    echo "    no CAP_NET_ADMIN on this login, so netem goes on a fresh network"
    echo "    namespace and the whole experiment runs inside it"
    exec unshare --user --map-root-user --net "$SCRIPT_PATH" "$@"
  fi
fi

cd "$PROJECT_ROOT"

# Loopback is down in a fresh namespace, and every endpoint is 127.0.0.1.
ip link set lo up

cleanup() {
  tc qdisc del dev lo root >/dev/null 2>&1 || true
}
trap cleanup EXIT

missing=0
for tool in tc ip ping python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: '$tool' not found." >&2
    missing=1
  fi
done
[[ "$missing" -eq 0 ]] || exit 1
if [[ ! -x "$BINARY" ]]; then
  echo "error: '$BINARY' not found; run 'cargo build --release' first." >&2
  exit 1
fi
if [[ ! -f "$CONFIG" ]]; then
  echo "error: config '$CONFIG' not found." >&2
  exit 1
fi

# Prove netem installs before spending an hour on runs that would all fail.
if ! tc qdisc replace dev lo root netem delay 0 limit "$LIMIT" 2>/dev/null; then
  echo "error: cannot install a netem qdisc on lo." >&2
  echo "       Without CAP_NET_ADMIN this script runs itself through unshare;" >&2
  echo "       if it got here anyway, unprivileged user namespaces are probably" >&2
  echo "       off (see kernel.unprivileged_userns_clone)." >&2
  exit 1
fi
cleanup

if [[ -e "$OUT_DIR" ]]; then
  echo "error: '$OUT_DIR' already exists; refusing to overwrite." >&2
  exit 1
fi
mkdir -p "$OUT_DIR/logs"
TSV="$OUT_DIR/netem.tsv"

echo "==> binary      : $BINARY"
echo "==> config      : $CONFIG (overridden: --nodes [$NODE_COUNTS] --keys $KEYS)"
echo "==> points      : nodes=[$NODE_COUNTS] delays=[$DELAYS] rates=[${RATES:-none}] concs=[$CONCS] reps=$REPS"
echo "==> output dir  : $OUT_DIR"
echo

# Measured RTT of the shaped loopback, in microseconds. ping's summary line is
# "rtt min/avg/max/mdev = 0.021/0.037/0.057/0.012 ms"; the average is what the
# table reports and what the slope is fitted against, converted from ms because
# the delay knobs are written in ns/us/ms and mixing the two invites a 1000x
# error in the fit.
measure_rtt_us() {
  local avg_ms
  avg_ms="$(ping -c "$PING_COUNT" -i 0.05 -q 127.0.0.1 2>/dev/null \
    | grep -o '= *[0-9.]*/[0-9.]*/[0-9.]*/[0-9.]* *ms' \
    | head -n 1 | cut -d' ' -f2 | cut -d/ -f2)"
  if [[ -z "$avg_ms" ]]; then
    echo ""
  else
    awk -v ms="$avg_ms" 'BEGIN { printf "%.0f", ms * 1000 }'
  fi
}

# netem's own drop count, so a run that lost packets can be told apart from one
# that was merely slow.
netem_drops() {
  local dropped
  dropped="$(tc -s qdisc show dev lo 2>/dev/null | grep -o 'dropped [0-9]*' | head -n 1 | awk '{print $2}')"
  if [[ -z "$dropped" ]]; then
    echo 0
  else
    echo "$dropped"
  fi
}

# One run's numbers, from the summary the experiment already writes. Column
# order matches the TSV header below. A failed run still gets a row.
read_run() {
  python3 - "$1" <<'PY'
import json, os, sys

d = sys.argv[1]
COLUMNS = ('success', 'elapsed_s', 'worker_threads', 'keys', 'put_s', 'put_kps',
           'getverify_kps', 'scale_out_mut_s', 'scale_in_mut_s', 'lat_p50_us',
           'lat_p95_us', 'lat_p99_us', 'owner_rpc_p50_us', 'full_rf_s', 'resv_rej',
           'rpc_retry', 'peak_q')

try:
    summary = json.load(open(os.path.join(d, 'summary.json')))
except Exception:
    print('\t'.join(['0'] * len(COLUMNS)))
    sys.exit(0)

m = summary.get('measurements') or {}
o = summary.get('observations') or {}

# What the run resolved to, not what was asked for: worker_threads defaults to
# CPUs / (nodes + 2), so it moves with the node count unless a config pins it,
# and a sweep over node counts must show that rather than assume it.
worker_threads = 0
try:
    manifest = json.load(open(os.path.join(d, 'manifest.json')))
    worker_threads = ((manifest.get('config') or {}).get('worker_threads')) or 0
except Exception:
    pass


def seconds(name):
    return (m.get(name) or {}).get('seconds') or 0.0


def rate(name):
    return (m.get(name) or {}).get('operations_per_second') or 0.0


def pct(block, key):
    return (o.get(block) or {}).get(key) or 0


pressure = o.get('node_pressure_after_initial_put') or {}


def tot(key):
    return sum((n.get(key) or 0) for n in pressure.values())


peak = max(((n.get('replication_stream_peak_pending') or 0)
            for n in pressure.values()), default=0)
keys = (m.get('initial_put') or {}).get('operations') or 0

print('\t'.join(str(x) for x in [
    1 if summary.get('success') else 0,
    summary.get('elapsed_seconds') or 0,
    worker_threads,
    keys,
    seconds('initial_put'),
    rate('initial_put'),
    rate('initial_get_verify'),
    seconds('scale_out_with_mutations'),
    seconds('scale_in_with_mutations'),
    pct('client_put_latency_us', 'p50'),
    pct('client_put_latency_us', 'p95'),
    pct('client_put_latency_us', 'p99'),
    pct('owner_rpc_put_latency_us', 'p50'),
    o.get('initial_full_rf_seconds') or 0,
    tot('replication_reservation_rejections'),
    tot('replication_rpc_retries'),
    peak,
]))
PY
}

printf "point\tnodes\trate\tdelay\tconc\trep\trtt_us\tsuccess\telapsed_s\tworker_threads\tkeys\tput_s\tput_kps\tgetverify_kps\tscale_out_mut_s\tscale_in_mut_s\tlat_p50_us\tlat_p95_us\tlat_p99_us\towner_rpc_p50_us\tfull_rf_s\tresv_rej\trpc_retry\tpeak_q\tcpu_s\tcores\tdrops\n" > "$TSV"

point_index=0
# One empty element, not an empty array: under `set -u` an empty array expands to
# zero iterations, which would skip the whole sweep when RATES is unset.
rate_list=("")
if [[ -n "$RATES" ]]; then
  read -r -a rate_list <<< "$RATES"
fi

for nodes in $NODE_COUNTS; do
for delay in $DELAYS; do
  for rate in "${rate_list[@]}"; do
    for conc in $CONCS; do
      point_index=$((point_index + 1))
      point="$point_index"

      if [[ "$delay" == "off" ]]; then
        if [[ -n "$rate" ]]; then
          echo "error: DELAYS=off with RATES is not a point; a rate limit needs a qdisc." >&2
          exit 2
        fi
        cleanup
      elif [[ -n "$rate" ]]; then
        tc qdisc replace dev lo root netem delay "$delay" rate "$rate" limit "$LIMIT"
      else
        tc qdisc replace dev lo root netem delay "$delay" limit "$LIMIT"
      fi
      rtt_us="$(measure_rtt_us)"
      if [[ -z "$rtt_us" ]]; then
        rtt_us=0
      fi

      printf "  point %-3s nodes=%-3s delay=%-6s rate=%-8s conc=%-4s RTT=%sus\n" \
        "$point" "$nodes" "$delay" "${rate:-none}" "$conc" "$rtt_us"

      for rep in $(seq 1 "$REPS"); do
        base="$OUT_DIR/logs/point$point-$rep"
        # --mode/--nodes/--keys/--concurrency win over the config file, so the
        # run keeps the config's replication factor, policy and byte budgets. The
        # mode is pinned because the columns read_run collects come from the
        # correctness flow.
        TIMEFORMAT='%3R %3U %3S'
        { time "$BINARY" experiment \
            --config "$CONFIG" --mode correctness --output "$base/run" \
            --nodes "$nodes" --keys "$KEYS" --concurrency "$conc" \
            >"$base.log" 2>&1; } 2>"$base.time" || true
        read -r real user sys < "$base.time" || true

        row="$(read_run "$base/run")"
        drops="$(netem_drops)"
        cpu_s="$(awk -v u="${user:-0}" -v s="${sys:-0}" 'BEGIN { printf "%.3f", u + s }')"
        cores="$(awk -v c="$cpu_s" -v r="${real:-0}" 'BEGIN { printf "%.2f", (r > 0 ? c / r : 0) }')"

        # read_run's 17 summary columns sit between the seven identifying columns
        # and the three the shell measures, which is the TSV header's order.
        # Eleven conversions for eleven arguments: bash's printf reuses the
        # format string when given more arguments than it has, which would print
        # the last one followed by a row of empty fields.
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
          "$point" "$nodes" "${rate:-none}" "$delay" "$conc" "$rep" "$rtt_us" \
          "$row" "$cpu_s" "$cores" "$drops" >> "$TSV"

        success="$(cut -f1 <<<"$row")"
        if [[ "$success" == "1" ]]; then
          printf "      rep %s  put=%s keys/s  p50=%sus  wt=%s  cores=%s  drops=%s\n" \
            "$rep" "$(cut -f6 <<<"$row")" "$(cut -f10 <<<"$row")" \
            "$(cut -f3 <<<"$row")" "$cores" "$drops"
        else
          printf "      rep %s  FAILED (see %s.log)\n" "$rep" "$base"
        fi

        if [[ "$KEEP_RUNS" != "1" ]]; then
          rm -rf "$base/run"
        fi
      done
    done
  done
done
done

cleanup

echo
echo "==> aggregating..."
python3 - "$OUT_DIR/netem.tsv" "$OUT_DIR/medians.tsv" "$OUT_DIR/analysis.txt" <<'PY'
import collections
import statistics
import sys

tsv, medians_path, analysis_path = sys.argv[1:4]

hdr = None
rows = []
for line in open(tsv):
    parts = line.rstrip('\n').split('\t')
    if not parts or not parts[0]:
        continue
    if parts[0] == 'point':
        hdr = parts
        continue
    rows.append(dict(zip(hdr, parts)))
if hdr is None or not rows:
    raise SystemExit('no rows in ' + tsv)

NUM = ('rtt_us', 'success', 'elapsed_s', 'worker_threads', 'cpu_s', 'cores', 'keys',
       'put_s', 'put_kps', 'getverify_kps', 'scale_out_mut_s', 'scale_in_mut_s',
       'lat_p50_us', 'lat_p95_us', 'lat_p99_us', 'owner_rpc_p50_us', 'full_rf_s',
       'resv_rej', 'rpc_retry', 'peak_q', 'drops')


def num(row, key):
    try:
        return float(row.get(key) or 0)
    except ValueError:
        return 0.0


points = collections.OrderedDict()
for row in rows:
    key = (row['point'], row['nodes'], row['rate'], row['delay'], row['conc'])
    points.setdefault(key, []).append(row)

medians = []
for (point, nodes, rate, delay, conc), rs in points.items():
    ok = [r for r in rs if num(r, 'success') == 1]
    m = collections.OrderedDict()
    m['point'] = point
    m['nodes'] = nodes
    m['rate'] = rate
    m['delay'] = delay
    m['conc'] = conc
    m['reps'] = len(rs)
    m['ok'] = len(ok)
    for key in NUM:
        vals = [num(r, key) for r in ok]
        m[key] = statistics.median(vals) if vals else 0.0
    # Drops are a verdict on the point, not a measurement to average: one lossy
    # repetition contaminates the point, and a median would hide it.
    m['drops'] = max((num(r, 'drops') for r in rs), default=0.0)
    if len(ok) > 1 and m['put_kps']:
        kps = [num(r, 'put_kps') for r in ok]
        m['put_kps_spread'] = (max(kps) - min(kps)) / m['put_kps'] * 100
    else:
        m['put_kps_spread'] = 0.0
    medians.append(m)

with open(medians_path, 'w') as f:
    f.write('\t'.join(medians[0].keys()) + '\n')
    for m in medians:
        f.write('\t'.join(
            (f"{v:.3f}" if isinstance(v, float) else str(v)) for v in m.values()) + '\n')

print(f"{'pt':>3} {'nodes':>5} {'delay':>6} {'rate':>8} {'conc':>4} {'RTT_us':>8} {'put_kps':>9} "
      f"{'spread':>7} {'p50_us':>7} {'p99_us':>7} {'wt':>3} {'cores':>6} {'ceil_kps':>9} "
      f"{'ceil_ratio':>10} {'getver_kps':>10} {'drops':>6}")
print('-' * 132)
for m in medians:
    # conc is the string read back out of the TSV, so it needs converting before
    # it can divide.
    ceil = int(m['conc']) / m['lat_p50_us'] * 1e6 if m['lat_p50_us'] else 0
    ratio = m['put_kps'] / ceil if ceil else 0
    print(f"{m['point']:>3} {m['nodes']:>5} {m['delay']:>6} {m['rate']:>8} {m['conc']:>4} "
          f"{m['rtt_us']:>8.0f} {m['put_kps']:>9.0f} {m['put_kps_spread']:>6.1f}% "
          f"{m['lat_p50_us']:>7.0f} {m['lat_p99_us']:>7.0f} {m['worker_threads']:>3.0f} "
          f"{m['cores']:>6.2f} {ceil:>9.0f} {ratio:>10.3f} {m['getverify_kps']:>10.0f} "
          f"{m['drops']:>6.0f}")

lines = []
lines.append('Rounded-trip sensitivity, and where the network becomes the limiter')
lines.append('')
lines.append('ceil_kps is --concurrency / p50, the throughput ceiling a purely')
lines.append('latency-bound path would reach, and ceil_ratio is the measured')
lines.append('throughput as a fraction of it. A ratio near 1 means the network path is')
lines.append('the limiter; a ratio far below 1 means something local is, and the')
lines.append('network is not what caps this configuration. wt is the worker_threads the')
lines.append('run resolved to, which matters when it varies with the node count.')
lines.append('')

dropped = [m for m in medians if m['drops'] > 0]
if dropped:
    lines.append('POINTS WITH DROPS, excluded from the fit: netem dropped packets, so')
    lines.append('these points measure loss and TCP retransmission rather than delay.')
    for m in dropped:
        lines.append(f"  point {m['point']} nodes={m['nodes']} delay={m['delay']} "
                     f"dropped={m['drops']:.0f}")
    lines.append('')

# Fitted per node count and concurrency, never pooled: the intercept is the local
# per-operation cost and it differs between configurations, so a pooled fit would
# average two different systems into one slope. "off" is excluded because with no
# qdisc it measures the absence of the instrument rather than a zero-delay network.
groups = collections.OrderedDict()
for m in medians:
    if m['delay'] == 'off' or m['rate'] != 'none' or m['rtt_us'] <= 0 or m['drops'] > 0:
        continue
    groups.setdefault((m['nodes'], m['conc']), []).append(m)

fits = []
for (nodes, conc), pts in groups.items():
    if len(pts) < 2:
        continue
    xs = [m['rtt_us'] for m in pts]
    ys = [m['lat_p50_us'] for m in pts]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    slope = sxy / sxx if sxx else 0.0
    intercept = my - slope * mx
    r = sxy / ((sxx * syy) ** 0.5) if sxx and syy else 0.0
    fits.append({'nodes': nodes, 'conc': conc, 'slope': slope, 'intercept': intercept,
                 'r': r, 'n': n})

if fits:
    lines.append('Latency against measured RTT, least squares per node count:')
    lines.append(f"{'nodes':>5} {'conc':>4} {'intercept_us':>13} {'slope':>7} {'r':>7} {'n':>3}")
    for f in fits:
        lines.append(f"{f['nodes']:>5} {f['conc']:>4} {f['intercept']:>13.0f} "
                     f"{f['slope']:>7.2f} {f['r']:>7.3f} {f['n']:>3}")
    lines.append('')
    lines.append('  The slope is how many round trips the sampled write path serializes')
    lines.append('  on. It should not depend on node count: a bigger ring spreads the')
    lines.append('  same writes over more owners, it does not add a hop. A slope that')
    lines.append('  drifts with node count means something else grew, and that is worth')
    lines.append('  chasing before reading anything else in this file.')
    lines.append('')

# The crossover: the RTT at which the network's own ceiling (concurrency over
# serialized latency) drops to the local ceiling measured at loopback RTT. Above it
# the network is the limiter. This is the quantity that decides whether a
# same-datacenter link matters, and it is what moves down as node count rises.
by_nodes = collections.OrderedDict()
for m in medians:
    if m['rate'] != 'none' or m['drops'] > 0 or m['put_kps'] <= 0:
        continue
    by_nodes.setdefault((m['nodes'], m['conc']), []).append(m)

cross = []
for (nodes, conc), pts in by_nodes.items():
    # Only "0": "off" measures the absence of the instrument, not a zero-delay
    # network, so it is a different system from the one the slope was fitted on.
    loopback = [m for m in pts if m['delay'] == '0']
    if not loopback:
        continue
    local_ceiling = max(m['put_kps'] for m in loopback)
    fit = next((f for f in fits if f['nodes'] == nodes and f['conc'] == conc), None)
    slope = fit['slope'] if fit else 0
    if not slope:
        continue
    cross.append({'nodes': nodes, 'conc': int(conc), 'ceiling': local_ceiling,
                  'slope': slope, 'rtt_us': int(conc) / (slope * local_ceiling) * 1e6})

if cross:
    lines.append('Crossover: the RTT at which the network ceiling falls to the')
    lines.append('local one, computed as concurrency / (slope * loopback ceiling).')
    lines.append('Above this RTT the network is the limiter; below it, it is not.')
    lines.append('')
    lines.append(f"{'nodes':>5} {'conc':>4} {'loopback_keys/s':>15} {'slope':>7} "
                 f"{'crossover_RTT_us':>17}")
    for c in cross:
        lines.append(f"{c['nodes']:>5} {c['conc']:>4} {c['ceiling']:>15.0f} "
                     f"{c['slope']:>7.2f} {c['rtt_us']:>17.0f}")
    lines.append('')
    lines.append('A same-datacenter LAN sits around 100 to 300 us of RTT. A crossover')
    lines.append('above that range means the network is not the limiter at this node')
    lines.append('count; a crossover below it means the network already is. The number')
    lines.append('to compare against is the rightmost column, not whether throughput')
    lines.append('fell at the largest delay tested.')
    lines.append('')

    # A crossover above the largest RTT actually swept cannot be located from
    # this data, only bounded. Say which case each point is.
    swept = max((m['rtt_us'] for m in medians
                 if m['rate'] == 'none' and m['delay'] != 'off'), default=0)
    for c in cross:
        verdict = ('crossover is inside the swept range' if c['rtt_us'] <= swept
                   else f'crossover is above the {swept:.0f}us swept, so only bounded')
        lines.append(f"  nodes={c['nodes']:>2} conc={c['conc']:>4}: {verdict}")
    lines.append('')

lines.append('Reading the two together separates the candidates:')
lines.append('  * throughput flat while latency rises about 1:1 -> the path does not')
lines.append('    wait on the network, and the serial cost is elsewhere: a lock, a')
lines.append('    timer, or the driver.')
lines.append('  * throughput falling with RTT while in-flight stays near --concurrency')
lines.append('    -> the write path is limited by offered concurrency over round')
lines.append('    trips, which is a network-path limit.')
lines.append('  * throughput falling and in-flight falling too -> something that does')
lines.append('    not scale with offered load is the limit.')
lines.append('  * CPU rising with RTT would mean the delays were turned into work; CPU')
lines.append('    falling is what waiting looks like.')
lines.append('')
lines.append('In-flight is keys/s times the sampled p50, and that p50 is a serial')
lines.append('unloaded put (128 of them, crates/hashring-experiment/src/lib.rs:613),')
lines.append('not the latency under --concurrency, so treat it as a sanity check on the')
lines.append('shape of the curve, not as a measurement. The slope is the trustworthy')
lines.append('quantity: it is a difference, so the missing queueing cancels.')
if not fits:
    lines.append('')
    lines.append('No group had two clean shaped points, so no slope was fitted.')

open(analysis_path, 'w').write('\n'.join(lines) + '\n')
print()
print('\n'.join(lines))
PY

echo "==> wrote:"
for file in "$TSV" "$OUT_DIR/medians.tsv" "$OUT_DIR/analysis.txt"; do
  printf '    %10s  %s\n' "$(du -h "$file" | cut -f1)" "$file"
done
echo "    raw rows: $TSV"
echo "    medians : $OUT_DIR/medians.tsv"
echo "    fit     : $OUT_DIR/analysis.txt"
