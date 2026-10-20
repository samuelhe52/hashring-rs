#!/usr/bin/env python3
"""Prints a paired comparison of the variants of one compare.py run.

    paired-summary.py <run-dir> [<run-dir> ...]

A run that measures several variants measures them in turn inside every
repetition, so each variant has a Redis control from the same repetition.
The ratio to that control is the number to compare: the host's load moves
both systems, and the ratio moves with them.
"""
import json
import os
import statistics
import sys


def load_rows(run):
    """The rows of a run, with the variant's settings."""
    with open(os.path.join(run, "metadata.json")) as f:
        metadata = json.load(f)
    variants = {v["name"]: v for v in metadata["settings"].get("variants", [])}
    with open(os.path.join(run, "measurements.csv")) as f:
        lines = f.read().strip().splitlines()
    header = lines[0].split(",")
    rows = []
    for line in lines[1:]:
        row = dict(zip(header, line.split(",")))
        for key in ("rps", "p50_ms", "p99_ms", "elapsed_seconds", "cpu_seconds",
                    "gateway_cpu_seconds", "node_cpu_seconds"):
            row[key] = float(row[key])
        row["rep"] = int(row["rep"])
        rows.append(row)
    return rows, variants


def pick(rows, case, workload, variant, system):
    """The rows of one case, workload, variant and system, by repetition."""
    return {
        row["rep"]: row
        for row in rows
        if row["case"] == case
        and row["workload"] == workload
        and row["variant"] == variant
        and row["system"] == system
    }


def ratio_of(rows, case, workload, variant, rep):
    """The hashring rate of a repetition over the control of that repetition."""
    control = pick(rows, case, workload, variant, "redis").get(rep)
    measured = pick(rows, case, workload, variant, "hashring").get(rep)
    if not control or not measured:
        return None
    return measured["rps"] / control["rps"]


def us_per_op(picked):
    """The CPU of the phase per request, and the gateway's and the nodes'."""
    ops = sum(row["rps"] * row["elapsed_seconds"] for row in picked.values())
    if ops <= 0:
        return 0.0, 0.0, 0.0
    return (
        1e6 * sum(row["cpu_seconds"] for row in picked.values()) / ops,
        1e6 * sum(row["gateway_cpu_seconds"] for row in picked.values()) / ops,
        1e6 * sum(row["node_cpu_seconds"] for row in picked.values()) / ops,
    )


def main():
    runs = sys.argv[1:]
    base = None
    if "--base" in runs:
        index = runs.index("--base")
        base = runs[index + 1]
        runs = runs[:index] + runs[index + 2:]
    if not runs:
        print(__doc__)
        return 1
    for run in runs:
        rows, variants = load_rows(run)
        print(f"=== {run}")
        for variant, settings in variants.items():
            print(
                f"  {variant:12s} pool={settings['pool']} batch={settings['batch']} "
                f"linger={settings['linger']}ms"
            )
        cases = sorted({row["case"] for row in rows})
        for case in cases:
            for workload in ("write", "read", "mixed"):
                for variant in variants:
                    picked = pick(rows, case, workload, variant, "hashring")
                    if not picked:
                        continue
                    # The ratio of the hashring side to the control of the
                    # same repetition. The control row itself is not part of
                    # the ratio.
                    ratios = [
                        ratio_of(rows, case, workload, variant, rep)
                        for rep in picked
                    ]
                    ratios = [r for r in ratios if r is not None]
                    rps = [row["rps"] for row in picked.values()]
                    p50 = [row["p50_ms"] for row in picked.values()]
                    cpu, gw, node = us_per_op(picked)
                    ratio = f"{statistics.median(ratios):.4f}" if ratios else "-"
                    line = (
                        f"  {case:8s} {workload:6s} {variant:12s} "
                        f"rps={statistics.median(rps):>9.0f} "
                        f"(min {min(rps):>9.0f} max {max(rps):>9.0f}) "
                        f"p50={statistics.median(p50):.3f}ms "
                        f"ratio={ratio} "
                        f"cpu/op={cpu:.1f}us (gw {gw:.1f} node {node:.1f})"
                    )
                    if base and variant != base and base in variants:
                        deltas = []
                        for rep in sorted(picked):
                            mine = ratio_of(rows, case, workload, variant, rep)
                            theirs = ratio_of(rows, case, workload, base, rep)
                            if mine is not None and theirs is not None:
                                deltas.append(mine - theirs)
                        if deltas:
                            line += (
                                f"  vs {base}: "
                                + " ".join(f"{d:+.4f}" for d in deltas)
                                + f" median {statistics.median(deltas):+.4f}"
                            )
                    print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
