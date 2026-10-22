#!/usr/bin/env python3
"""Turn the output of `scripts/latency-sweep.sh` into tables.

The experiment writes a `summary.json` per run, with one measurement per
target rate. This reads those and prints one row per stage, and the medians
over the repetitions of a stage.

Usage:
    extract.py <results-dir>              one row per stage
    extract.py --medians <results-dir>    one row per stage, medians
    extract.py --capacity <results-dir>   the capacity run
"""

import json
import os
import statistics
import sys

ERROR_REASONS = ["deadline", "operation", "rpc", "topology", "not_found", "protocol"]
OPERATIONS = ["get", "put", "delete"]

# The columns of the table the acceptance report holds.
MEDIAN_COLUMNS = [
    "kind",
    "distribution",
    "virtual_nodes",
    "target_rate",
    "reps",
    "achieved_rate",
    "get_p50",
    "get_p95",
    "get_p99",
    "get_p999",
    "get_max",
    "late",
    "retries",
    "saturated_runs",
    "host_preload_ops",
    "owned_mom",
    "owned_cv",
    "ops_mom",
    "ops_cv",
    "cpu_max_percent",
]


def load_runs(results_dir):
    """Every latency run in the results directory, with its summary."""
    runs = []
    for name in sorted(os.listdir(results_dir)):
        directory = os.path.join(results_dir, name)
        summary_path = os.path.join(directory, "summary.json")
        manifest_path = os.path.join(directory, "manifest.json")
        if not os.path.isfile(summary_path) or not os.path.isfile(manifest_path):
            continue
        with open(summary_path) as handle:
            summary = json.load(handle)
        with open(manifest_path) as handle:
            manifest = json.load(handle)
        config = manifest.get("config") or {}
        if config.get("mode") != "latency":
            continue
        summary["config"] = config
        runs.append((name, summary))
    return runs


def run_kind(name):
    """What a run is for. The names come from `scripts/latency-sweep.sh`."""
    if name.startswith("vnodes-"):
        return "vnode"
    if name == "ladder":
        return "ladder"
    return "campaign"


def stage_row(run, summary, stage, measurement):
    """One stage of one run, as a flat row."""
    latency = measurement.get("latency_us") or {}
    balance = measurement.get("load_balance") or {}
    per_node = balance.get("per_node") or {}
    cpus = [node.get("cpu_percent") for node in per_node.values()]
    cpu_complete = bool(cpus) and all(value is not None for value in cpus)
    row = {
        "run": run,
        "kind": run_kind(run),
        "distribution": (summary.get("config") or {}).get("key_distribution"),
        "virtual_nodes": (summary.get("config") or {}).get("virtual_nodes"),
        "stage": stage,
        "target_rate": measurement.get("target_rate"),
        "achieved_rate": round(measurement.get("achieved_rate") or 0.0, 1),
        "seconds": round(measurement.get("seconds") or 0.0, 1),
        "sender_saturated": measurement.get("sender_saturated"),
        "issued": measurement.get("issued"),
        "late": measurement.get("late"),
        "retries": measurement.get("retries"),
        "owned_mom": round(balance.get("owned_keys_max_over_mean") or 0.0, 3),
        "owned_cv": round(balance.get("owned_keys_cv") or 0.0, 4),
        "ops_mom": round(balance.get("ops_handled_max_over_mean") or 0.0, 3),
        "ops_cv": round(balance.get("ops_handled_cv") or 0.0, 4),
        "cpu_max_percent": round(max(cpus), 1) if cpu_complete else None,
        "cpu_sum_percent": round(sum(cpus), 1) if cpu_complete else None,
        "nodes": len(per_node),
        "verified_keys": (summary.get("observations") or {}).get("latency_verified_keys"),
        # The preload of the run is the same workload every time, so it is the
        # run's own measure of how fast the host was.
        "host_preload_ops": (summary.get("observations") or {}).get("initial_put", {}).get("operations_per_second"),
    }
    ops = measurement.get("ops") or {}
    for operation in OPERATIONS:
        row[f"ops_{operation}"] = ops.get(operation)
    errors = measurement.get("errors") or {}
    for reason in ERROR_REASONS:
        row[f"err_{reason}"] = errors.get(reason, 0)
    for operation in OPERATIONS:
        summary_us = latency.get(operation) or {}
        for percentile in ("p50", "p95", "p99", "p999", "max"):
            row[f"{operation}_{percentile}"] = summary_us.get(percentile)
        row[f"{operation}_samples"] = summary_us.get("samples")
    return row


def rows_of(runs):
    rows = []
    for name, summary in runs:
        for stage, measurement in (summary.get("measurements") or {}).items():
            if "target_rate" not in measurement:
                continue
            rows.append(stage_row(name, summary, stage, measurement))
    return rows


def print_table(rows, columns=None):
    if not rows:
        return
    columns = columns or list(rows[0].keys())
    print("\t".join(columns))
    for row in rows:
        print("\t".join("" if row.get(column) is None else str(row.get(column)) for column in columns))


def median_rows(rows):
    """One row per stage, over the repetitions of it."""
    groups = {}
    for row in rows:
        key = (row["kind"], row["distribution"], row["virtual_nodes"], row["target_rate"])
        groups.setdefault(key, []).append(row)
    out = []
    for key, members in sorted(groups.items(), key=lambda item: (item[0][0], str(item[0][1]), item[0][3])):
        first = members[0]
        row = {
            "kind": key[0],
            "distribution": key[1],
            "virtual_nodes": key[2],
            "target_rate": key[3],
            "reps": len(members),
            "runs": ",".join(sorted(member["run"] for member in members)),
            "saturated_runs": sum(1 for member in members if member.get("sender_saturated")),
        }
        for column in first:
            if column in ("run", "kind", "distribution", "virtual_nodes", "stage", "target_rate"):
                continue
            values = [member[column] for member in members if isinstance(member.get(column), (int, float))]
            if not values:
                row[column] = None
                continue
            row[column] = round(statistics.median(values), 3)
        # The spread of the two numbers a reader will argue about.
        for column in ("achieved_rate", "get_p99", "get_p50", "get_max", "late", "retries"):
            values = [member[column] for member in members if isinstance(member.get(column), (int, float))]
            if values:
                row[f"{column}_min"] = min(values)
                row[f"{column}_max"] = max(values)
        out.append(row)
    return out


def capacity_rows(results_dir):
    rows = []
    for name in sorted(os.listdir(results_dir)):
        summary_path = os.path.join(results_dir, name, "summary.json")
        manifest_path = os.path.join(results_dir, name, "manifest.json")
        if not os.path.isfile(summary_path) or not os.path.isfile(manifest_path):
            continue
        with open(summary_path) as handle:
            summary = json.load(handle)
        with open(manifest_path) as handle:
            manifest = json.load(handle)
        if (manifest.get("config") or {}).get("mode") != "performance":
            continue
        for stage, measurement in (summary.get("measurements") or {}).items():
            rows.append({
                "run": name,
                "stage": stage,
                "operations": measurement.get("operations"),
                "seconds": round(measurement.get("seconds") or 0.0, 3),
                "operations_per_second": round(measurement.get("operations_per_second") or 0.0, 1),
            })
    return rows


def markdown_table(rows, columns):
    """A table a document can hold."""
    if not rows:
        return ""
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        lines.append("| " + " | ".join("" if row.get(column) is None else str(row.get(column)) for column in columns) + " |")
    return "\n".join(lines)


def campaign_table(rows):
    """The latency table of the report: every repetition, with the speed of
    the host the run landed on."""
    lines = [
        "| Run | Distribution | Rate | Host | p50 | p95 | p99 | p999 | max |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        if row["kind"] != "campaign":
            continue
        lines.append(
            "| {run} | {distribution} | {rate} | {host} | {p50} | {p95} | {p99} | {p999} | {maximum} |".format(
                run=row["run"],
                distribution=row["distribution"],
                rate=f"{int(row['target_rate']):,}",
                host=f"{int(row['host_preload_ops']):,}",
                p50=f"{row['get_p50']:,.0f} us",
                p95=f"{row['get_p95']:,.0f} us",
                p99=f"{row['get_p99']:,.0f} us",
                p999=f"{row['get_p999']:,.0f} us",
                maximum=f"{row['get_max']:,.0f} us",
            )
        )
    return "\n".join(lines)


def balance_table(rows):
    """The load balance table of the report."""
    lines = [
        "| Distribution | Rate | Owned keys, max over mean | Owned keys, cv | Operations, max over mean | Operations, cv | Busiest node's CPU |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        if row["kind"] != "campaign":
            continue
        lines.append(
            "| {distribution} | {rate} | {owned} | {owned_cv} | {ops} | {ops_cv} | {cpu} |".format(
                distribution=row["distribution"],
                rate=f"{int(row['target_rate']):,}",
                owned=f"{row['owned_mom']:.3f}",
                owned_cv=f"{row['owned_cv']:.3f}",
                ops=f"{row['ops_mom']:.3f}",
                ops_cv=f"{row['ops_cv']:.3f}",
                cpu=f"{row['cpu_max_percent']:.1f}%" if row.get('cpu_max_percent') is not None else "unavailable",
            )
        )
    return "\n".join(lines)


def ladder_table(rows):
    """The ladder table of the report."""
    lines = [
        "| Target rate | Achieved | p50 | p95 | p99 | p999 | Late | Sender saturated |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        if row["kind"] != "ladder":
            continue
        lines.append(
            "| {rate} | {achieved} | {p50} | {p95} | {p99} | {p999} | {late} | {saturated} |".format(
                rate=f"{int(row['target_rate']):,}",
                achieved=f"{row['achieved_rate']:,.0f}",
                p50=f"{row['get_p50']:,.0f} us",
                p95=f"{row['get_p95']:,.0f} us",
                p99=f"{row['get_p99']:,.0f} us",
                p999=f"{row['get_p999']:,.0f} us",
                late=f"{int(row['late']):,}",
                saturated="yes" if row["sender_saturated"] else "no",
            )
        )
    return "\n".join(lines)


def vnode_table(rows):
    """The virtual node table of the report."""
    lines = [
        "| Virtual nodes | Owned keys, max over mean | Owned keys, cv | Operations, max over mean | Operations, cv |",
        "| --- | --- | --- | --- | --- |",
    ]
    seen = {}
    for row in rows:
        if row["kind"] != "vnode" or row["virtual_nodes"] in seen:
            continue
        seen[row["virtual_nodes"]] = row
    for vnodes, row in sorted(seen.items()):
        lines.append(
            "| {vnodes} | {owned} | {owned_cv} | {ops} | {ops_cv} |".format(
                vnodes=vnodes,
                owned=f"{row['owned_mom']:.3f}",
                owned_cv=f"{row['owned_cv']:.3f}",
                ops=f"{row['ops_mom']:.3f}",
                ops_cv=f"{row['ops_cv']:.3f}",
            )
        )
    return "\n".join(lines)


def main():
    arguments = sys.argv[1:]
    if not arguments:
        print(__doc__, file=sys.stderr)
        return 1
    mode = "stages"
    if arguments[0].startswith("--"):
        mode = arguments[0][2:]
        arguments = arguments[1:]
    results_dir = arguments[0]
    if mode == "capacity":
        print_table(capacity_rows(results_dir))
        return 0
    rows = rows_of(load_runs(results_dir))
    if mode == "medians":
        print_table(median_rows(rows))
    elif mode == "markdown":
        print(markdown_table(median_rows(rows), MEDIAN_COLUMNS))
    elif mode == "report":
        # The campaign is shown repetition by repetition: the host's speed
        # moves the latencies, so the spread is the measurement. The rest is
        # over the repetitions, because those numbers do not move.
        medians = median_rows(rows)
        print(campaign_table(rows))
        print()
        print(balance_table(medians))
        print()
        print(ladder_table(medians))
        print()
        print(vnode_table(medians))
    else:
        print_table(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
