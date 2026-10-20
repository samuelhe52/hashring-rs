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
        row["variant"] = row["variant"]
        for key in ("rps", "p50_ms", "p99_ms", "elapsed_seconds", "cpu_seconds",
                    "gateway_cpu_seconds", "node_cpu_seconds"):
            row[key] = float(row[key])
        row["rep"] = int(row["rep"])
        rows.append(row)
    return rows, variants


def main():
    runs = sys.argv[1:]
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
                    picked = [
                        row
                        for row in rows
                        if row["case"] == case
                        and row["workload"] == workload
                        and row["variant"] == variant
                    ]
                    if not picked:
                        continue
                    # The ratio of the hashring side to the control of the
                    # same repetition. The control row itself is not part of
                    # the ratio.
                    ratios = []
                    for row in picked:
                        if row["system"] != "hashring":
                            continue
                        control = next(
                            (
                                other["rps"]
                                for other in rows
                                if other["case"] == case
                                and other["workload"] == workload
                                and other["variant"] == variant
                                and other["system"] == "redis"
                                and other["rep"] == row["rep"]
                            ),
                            None,
                        )
                        if control:
                            ratios.append(row["rps"] / control)
                    rps = [row["rps"] for row in picked if row["system"] == "hashring"]
                    p50 = [row["p50_ms"] for row in picked if row["system"] == "hashring"]
                    if not rps:
                        continue
                    ops = sum(
                        row["rps"] * row["elapsed_seconds"]
                        for row in picked
                        if row["system"] == "hashring"
                    )
                    cpu = sum(row["cpu_seconds"] for row in picked if row["system"] == "hashring")
                    gw = sum(
                        row["gateway_cpu_seconds"]
                        for row in picked
                        if row["system"] == "hashring"
                    )
                    node = sum(
                        row["node_cpu_seconds"] for row in picked if row["system"] == "hashring"
                    )
                    ratio = f"{statistics.median(ratios):.4f}" if ratios else "-"
                    print(
                        f"  {case:8s} {workload:6s} {variant:12s} "
                        f"rps={statistics.median(rps):>9.0f} "
                        f"(min {min(rps):>9.0f} max {max(rps):>9.0f}) "
                        f"p50={statistics.median(p50):.3f}ms "
                        f"ratio={ratio} "
                        f"cpu/op={1e6 * cpu / ops:.1f}us "
                        f"(gw {1e6 * gw / ops:.1f} node {1e6 * node / ops:.1f})"
                    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
