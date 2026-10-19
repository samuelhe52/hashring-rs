#!/usr/bin/env python3
"""Usage: cpu-window.py <run_dir> <cpu.tsv>
Prints per-role CPU cores (user, sys) averaged over the initial_put and initial_get_verify windows."""
import json, sys, collections
run, tsv = sys.argv[1], sys.argv[2]
ev = {}
for line in open(f"{run}/events.jsonl"):
    e = json.loads(line); ev[e["event"]] = e
def win(name):
    e = ev[name]; end = e["unix_ms"]; return end - e["fields"]["seconds"] * 1000, end
def secs(t):
    p = t.split(":"); return float(p[0]) * 60 + float(p[1]) if len(p) == 2 else float(t)
rows = collections.defaultdict(list)
for i, line in enumerate(open(tsv)):
    if i == 0: continue
    ms, pid, role, u, s = line.rstrip("\n").split("\t")
    rows[(pid, role)].append((int(ms), secs(u), secs(s)))
def at(samples, t):
    best = samples[0]
    for x in samples:
        if x[0] <= t: best = x
    return best
for name in ("initial_put_complete", "initial_get_verify_complete"):
    if name not in ev: continue
    a, b = win(name); dur = (b - a) / 1000
    print(f"== {name}: {dur:.2f}s window")
    tu = ts = 0
    for (pid, role), smp in sorted(rows.items(), key=lambda k: k[0][1]):
        x, y = at(smp, a), at(smp, b)
        du, ds = (y[1] - x[1]) / dur, (y[2] - x[2]) / dur
        tu += du; ts += ds
        print(f"  {role:14s} user {du:5.2f}  sys {ds:5.2f}  cores {du+ds:5.2f}")
    print(f"  {'TOTAL':14s} user {tu:5.2f}  sys {ts:5.2f}  cores {tu+ts:5.2f}  sys_share {ts/max(tu+ts,1e-9):.0%}")
