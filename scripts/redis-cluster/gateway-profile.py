#!/usr/bin/env python3
"""Measure the node-side handler time of the traffic the Redis gateway sends.

The gateway is a client of the cluster, so every Redis command becomes a
cluster RPC. This script starts a hashring-rs cluster with
HASHRING_PROFILE_RPC=1, drives writes and reads through the gateway, and
reports what the nodes spent inside their handlers against what the clients
waited for. The difference is the transport, the extra hop through the
gateway, and the scheduling in between.

Usage:
    gateway-profile.py --output DIR [--nodes 3] [--rf 1] [--gateways 1]
                       [--requests 200000] [--keyspace 300000]
"""

import argparse
import collections
import csv
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
HASHRING_SCRIPT = os.path.join(HERE, "hashring-cluster.sh")


def read_profile(log_dir):
    """The last rpc_profile line per (file, histogram)."""
    final = {}
    for name in sorted(os.listdir(log_dir)):
        if not name.startswith("node-") or not name.endswith(".log"):
            continue
        path = os.path.join(log_dir, name)
        for line in open(path, errors="replace"):
            if not line.startswith("rpc_profile "):
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            _, ts, role, hist, count, sum_ns = parts[:6]
            buckets = dict(map(int, kv.split(":")) for kv in parts[6].split(",")) if len(parts) > 6 else {}
            final[(name, hist)] = (role, int(count), int(sum_ns), buckets)
    return final


def subtract(before, after):
    """The counts of a phase: the later cumulative values minus the earlier."""
    out = {}
    for key, (role, count, sum_ns, buckets) in after.items():
        # A histogram that had no samples before the phase starts at zero.
        old = before.get(key, (role, 0, 0, {}))
        d_buckets = collections.Counter()
        for bucket, n in buckets.items():
            d = n - old[3].get(bucket, 0)
            if d > 0:
                d_buckets[bucket] = d
        out.setdefault(key[1], [role, 0, 0, collections.Counter()])
        out[key[1]][1] += count - old[1]
        out[key[1]][2] += sum_ns - old[2]
        out[key[1]][3].update(d_buckets)
    return out


def percentile(buckets, n, q):
    target = q * n
    run = 0
    for i in sorted(buckets):
        run += buckets[i]
        if run >= target:
            return 2 ** ((i + 0.5) / 4)
    return float("nan")


def print_table(title, agg):
    print(f"== {title}")
    print(f"{'histogram':32s} {'count':>10s} {'mean us':>9s} {'p50 us':>9s} {'p99 us':>9s}")
    for name, (role, n, s, b) in sorted(agg.items()):
        if n == 0:
            continue
        print(f"{name:32s} {n:10d} {s / n / 1000.0:9.1f} {percentile(b, n, .5):9.1f} {percentile(b, n, .99):9.1f}")
    print()


def prefill(redis_bin, port, keyspace, value):
    """Writes every key the read phase can ask for, so its reads are hits.

    redis-benchmark draws __rand_int__ from [0, keyspace) and prints it as
    twelve digits, so these are exactly the keys of the templates below."""
    lines = []
    for i in range(keyspace):
        key = f"k:{{tag}}:{i:012d}"
        lines.append(f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n")
    cli = os.path.join(redis_bin, "redis-cli")
    proc = subprocess.run([cli, "-h", "127.0.0.1", "-p", str(port), "--pipe"],
                          input="".join(lines), capture_output=True, text=True)
    if proc.returncode != 0 or "errors: 0," not in proc.stdout:
        raise RuntimeError(f"prefill failed:\n{proc.stdout}\n{proc.stderr}")
    size = int(subprocess.run([cli, "-h", "127.0.0.1", "-p", str(port), "dbsize"],
                              capture_output=True, text=True, check=True).stdout)
    if size != keyspace:
        raise RuntimeError(f"prefill left {size} keys, expected {keyspace}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", required=True)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--rf", type=int, default=1)
    parser.add_argument("--gateways", type=int, default=1)
    parser.add_argument("--requests", type=int, default=200000)
    parser.add_argument("--keyspace", type=int, default=300000)
    parser.add_argument("--value-bytes", type=int, default=128)
    parser.add_argument("--redis-bin", default=os.environ.get("REDIS_BIN_DIR", "/usr/bin"))
    parser.add_argument("--hashring-bin", default=os.path.join(HERE, "..", "..", "target", "release", "hashring-rs"))
    args = parser.parse_args()

    output = os.path.abspath(args.output)
    if os.path.exists(output) and os.listdir(output):
        parser.error(f"{output} exists and is not empty")
    os.makedirs(output, exist_ok=True)
    work = os.path.join(output, "work")
    os.makedirs(work)
    cluster_dir = os.path.join(work, "hashring")

    env = {**os.environ, "HASHRING_PROFILE_RPC": "1", "HASHRING_BIN": os.path.abspath(args.hashring_bin)}
    subprocess.run(
        ["bash", HASHRING_SCRIPT, "start", cluster_dir, str(args.nodes), str(args.rf), str(args.gateways)],
        check=True, env=env,
    )
    benchmark = os.path.join(args.redis_bin, "redis-benchmark")
    # redis-benchmark expands only __rand_int__ in a custom command; __data__
    # and -d apply to its built-in tests, so the value is built here.
    value = "v" * args.value_bytes
    try:
        for workload, command in (
            ("write", ["set", "k:{tag}:__rand_int__", value]),
            ("read", ["get", "k:{tag}:__rand_int__"]),
        ):
            if workload == "read":
                # The write phase leaves about 1 - e^(-requests/keyspace) of
                # the key space written, half of it with the defaults.
                prefill(args.redis_bin, 6399, args.keyspace, value)
                # The nodes log their histograms every 500 ms; the prefill's
                # writes must be in the snapshot before the phase.
                time.sleep(1)
            before = read_profile(cluster_dir)
            started = time.time()
            proc = subprocess.run(
                [benchmark, "-h", "127.0.0.1", "-p", "6399", "-c", "64", "-r", str(args.keyspace),
                 "-n", str(args.requests), "--csv"] + command,
                capture_output=True, text=True,
            )
            elapsed = time.time() - started
            time.sleep(1)
            after = read_profile(cluster_dir)
            agg = subtract(before, after)
            print_table(f"{workload} phase, {elapsed:.1f}s, {proc.stdout.strip().splitlines()[-1]}", agg)
    finally:
        subprocess.run(["bash", HASHRING_SCRIPT, "stop", cluster_dir], check=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
