#!/usr/bin/env python3
"""Usage: rpc-profile.py <run_dir>
Reads HASHRING_PROFILE_RPC=1 output (rpc_profile lines) from <run_dir>/process-logs/node-*.stderr.log
and <run_dir>.stderr (driver), takes each histogram's final cumulative value (performance mode has
one PUT phase then one GET phase, and each histogram belongs to one phase), sums nodes, and prints
count, mean and p50/p90/p99 (bucket midpoints, ~9% resolution). replication_batch_entries values are
entries, not microseconds."""
import sys, glob, os, collections
run = sys.argv[1].rstrip('/')
files = sorted(glob.glob(os.path.join(run, 'process-logs', 'node-*.stderr.log'))) + [run + '.stderr']
final = {}
for f in files:
    if not os.path.exists(f): continue
    for line in open(f, errors='replace'):
        if not line.startswith('rpc_profile '): continue
        parts = line.split()
        if len(parts) < 6: continue
        _, ts, role, name, count, sum_ns = parts[:6]
        buckets = dict(map(int, kv.split(':')) for kv in parts[6].split(',')) if len(parts) > 6 else {}
        final[(f, name)] = (role, int(count), int(sum_ns), buckets)
agg = collections.defaultdict(lambda: [None, 0, 0, collections.Counter()])
for (f, name), (role, c, s, b) in final.items():
    a = agg[name]; a[0] = role; a[1] += c; a[2] += s; a[3].update(b)
def pct(b, n, q):
    target = q * n; run_ = 0
    for i in sorted(b):
        run_ += b[i]
        if run_ >= target: return 2 ** ((i + 0.5) / 4)
    return float('nan')
order = ['client_put_operation', 'client_put_rpc', 'node_put_handler', 'owner_replication_rpc',
         'node_replicate_batch_handler', 'replication_batch_entries', 'client_get_rpc', 'node_get_handler']
print(f"{'histogram':30s} {'role':7s} {'count':>9s} {'mean':>9s} {'p50':>9s} {'p90':>9s} {'p99':>9s}  (µs)")
for name in order + sorted(set(agg) - set(order)):
    if name not in agg: continue
    role, n, s, b = agg[name]
    unit = 1000.0
    print(f"{name:30s} {role:7s} {n:9d} {s/n/unit:9.1f} {pct(b,n,.5):9.1f} {pct(b,n,.9):9.1f} {pct(b,n,.99):9.1f}")
