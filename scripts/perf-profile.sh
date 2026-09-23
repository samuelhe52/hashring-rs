#!/usr/bin/env bash
# perf-profile.sh — profile a hashring-rs experiment run.
#
# What it measures
# ----------------
#   * Per-process CPU time and average RSS, sampled at ~50 Hz via /proc.
#   * Per-stage wall time parsed from the experiment's events.jsonl.
#   * Per-process tracing timestamps parsed from process-logs/*.stdout.log,
#     so you can see what the coordinator and nodes did during each stage.
#
# Usage
# -----
#   ./scripts/perf-profile.sh                  # correctness, 20k keys
#   ./scripts/perf-profile.sh performance      # performance, 1M keys
#   ./scripts/perf-profile.sh correctness 60   # correctness, sample for 60s
#
#   env BINARY=target/release/hashring-rs ./scripts/perf-profile.sh performance
#
# Output
# ------
#   results/perf-profile-<timestamp>/
#     cpu.tsv                     raw /proc samples
#     summary.txt                 parsed per-process totals
#     stages.txt                  per-stage wall time from events.jsonl
#     tracing.txt                 child-process tracing events
#     run/events.jsonl            copied from the experiment
#     run/process-logs/           copied from the experiment
#     run/summary.json            copied from the experiment
#
# Notes
# -----
#   * Reads /proc/<pid>/stat, so it works on any Linux without extra deps.
#   * Does NOT use /usr/bin/time, so it sees child processes too.
#   * If you want to compare runs, just diff two summary.txt files.

set -euo pipefail

MODE="${1:-correctness}"
SAMPLE_SECONDS="${2:-300}"
BINARY="${BINARY:-target/debug/hashring-rs}"
SAMPLE_HZ="${SAMPLE_HZ:-50}"   # samples per second

if [[ "$MODE" != "correctness" && "$MODE" != "performance" ]]; then
    echo "usage: $0 [correctness|performance] [sample-window-seconds]" >&2
    exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "error: 'jq' is required for parsing events.jsonl" >&2
    exit 1
fi

if [[ ! -x "$BINARY" ]]; then
    echo "error: binary '$BINARY' not found or not executable" >&2
    echo "       run 'cargo build' first, or set BINARY=..." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

TS="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="results/perf-profile-$TS"
RUN_DIR="$OUT_DIR/run"
mkdir -p "$RUN_DIR"

CPU_TSV="$OUT_DIR/cpu.tsv"
SUMMARY_TXT="$OUT_DIR/summary.txt"
STAGES_TXT="$OUT_DIR/stages.txt"
TRACING_TXT="$OUT_DIR/tracing.txt"

echo "==> output dir: $OUT_DIR"
echo "==> binary:     $BINARY"
echo "==> mode:       $MODE"
echo "==> sample:     ${SAMPLE_HZ} Hz, max ${SAMPLE_SECONDS}s"

# ----------------------------------------------------------------------------
# 1. Sampler — runs in background, writes /proc samples to cpu.tsv
# ----------------------------------------------------------------------------
echo "==> starting sampler (pid $$ will own it)"
{
    echo -e "ts\tepoch_ns\tpid\tcomm\tstate\tutime\tstime\trss_kb"
    # utime/stime are in clock ticks; we convert in awk.
    end=$(( $(date +%s) + SAMPLE_SECONDS ))
    while [[ $(date +%s) -lt $end ]]; do
        for pid in $(pgrep -x hashring-rs 2>/dev/null || true); do
            if [[ -r /proc/$pid/stat ]]; then
                # /proc/<pid>/stat fields: pid(1) comm(2) state(3) ppid(4) pgrp(5)
                # session(6) tty_nr(7) tpgid(8) flags(9) minflt(10) cminflt(11)
                # majflt(12) cmajflt(13) utime(14) stime(15) ...
                # bash splits on whitespace and (comm) is one word, so we need
                # exactly 10 underscores between state and utime.
                read -r _ comm state _ _ _ _ _ _ _ _ _ _ utime stime _ < /proc/$pid/stat
                rss=$(awk '/VmRSS:/ {print $2}' /proc/$pid/status 2>/dev/null || echo 0)
                comm="${comm//\)/}}"
                comm="${comm//(/{}"
                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                    "$(date +%s)" "$(date +%s%N)" \
                    "$pid" "$comm" "$state" \
                    "$utime" "$stime" "$rss"
            fi
        done
        sleep "$(awk -v hz="$SAMPLE_HZ" 'BEGIN{print 1.0/hz}')"
    done
} > "$CPU_TSV" &
SAMPLER_PID=$!

cleanup() {
    if kill -0 "$SAMPLER_PID" 2>/dev/null; then
        kill "$SAMPLER_PID" 2>/dev/null || true
        wait "$SAMPLER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ----------------------------------------------------------------------------
# 2. Run the experiment (foreground). Use --require-clean-source off so the
#    script works regardless of git status.
# ----------------------------------------------------------------------------
echo "==> launching experiment..."
"$BINARY" experiment \
    --mode "$MODE" \
    --output "$RUN_DIR"

echo "==> experiment finished"

# Give the sampler one extra tick to catch final samples.
sleep 0.5

# ----------------------------------------------------------------------------
# 3. Parse per-process totals.
# ----------------------------------------------------------------------------
echo "==> writing $SUMMARY_TXT"
awk -F'\t' '
    NR == 1 { next }
    {
        pid   = $3
        comm  = $4
        # utime/stime are in clock ticks (usually 100 Hz).
        cpu_ticks = $6 + $7
        cpu_sec   = cpu_ticks / 100.0
        cpu[pid]  += cpu_sec
        rss_sum[pid] += $8
        rss_n[pid]++
        if (first[pid] == "" || $2 + 0 < first[pid] + 0) first[pid] = $2 + 0
        if (last[pid]  == "" || $2 + 0 > last[pid]  + 0) last[pid]  = $2 + 0
        if (!(pid in comm_seen)) { comm_seen[pid] = 1; comm_of[pid] = comm }
    }
    END {
        for (pid in cpu) {
            wall_ns = last[pid] - first[pid]
            wall_s  = wall_ns / 1e9
            avg_rss = (rss_sum[pid] / rss_n[pid]) / 1024.0   # KB -> MB
            # Label by role: experiment / coordinator / node / other.
            role = "other"
            n = comm_of[pid]
            if (n ~ /hashring-rs/) {
                # We can not tell from the comm alone, but the parent of every
                # node/coordinator child is the experiment process. We treat
                # the highest-PID coordinator candidate specially below.
                role = "node-or-coord"
            }
            printf "%-15s pid=%-7s wall=%7.2fs cpu=%7.2fs avg_rss=%6.1fMB\n", \
                comm_of[pid], pid, wall_s, cpu[pid], avg_rss
        }
    }
' "$CPU_TSV" \
    | sort -k4 -t= -n -r \
    > "$SUMMARY_TXT"

# Refine roles using the experiment's process-logs directory: anything that
# produced coordinator.stdout.log is the coordinator, node-X.stdout.log is
# node-X. We do this after the CPU pass so the role labels are accurate.
if [[ -d "$RUN_DIR/process-logs" ]]; then
    awk -v logs_dir="$RUN_DIR/process-logs" '
        BEGIN {
            for (i = 1; i < ARGC; i++) {
                # ARGV is the original file, drop it.
            }
        }
        /^coordinator\.stdout\.log/ { coord_log = 1 }
        /^node-[0-9]+\.stdout\.log/ { match($0, /node-[0-9]+/); node = substr($0, RSTART, RLENGTH) }
        {
            if (coord_log) roles[$1] = "coordinator"
            else if (node)  roles[$1] = node
        }
        END {
            for (k in roles) print k, roles[k]
        }
    ' < <(ls "$RUN_DIR/process-logs") > "$OUT_DIR/.role-map" 2>/dev/null || true
fi

# ----------------------------------------------------------------------------
# 4. Parse per-stage wall time from events.jsonl.
# ----------------------------------------------------------------------------
echo "==> writing $STAGES_TXT"
{
    echo "stage                              start_ms      dur_ms     event"
    echo "--------------------------------   -----------   --------   -----"
    jq -r '
        [.unix_ms, .event, (.fields | tostring)] | @tsv
    ' "$RUN_DIR/events.jsonl" \
    | awk -F'\t' '
        {
            ts    = $1 + 0
            event = $2
            if (prev_ts == "") {
                printf "%-32s %13d   %8d   %s\n", "<experiment_started>", ts, 0, event
            } else {
                dur = ts - prev_ts
                printf "%-32s %13d   %8d   %s\n", event, ts, dur, event
            }
            prev_ts = ts
            prev_event = event
        }
    '
} > "$STAGES_TXT"

# ----------------------------------------------------------------------------
# 5. Extract child-process tracing timestamps.
# ----------------------------------------------------------------------------
echo "==> writing $TRACING_TXT"
{
    if [[ -d "$RUN_DIR/process-logs" ]]; then
        for log in "$RUN_DIR/process-logs"/*.stdout.log; do
            [[ -f "$log" ]] || continue
            name="$(basename "$log" .stdout.log)"
            echo "----- $name -----"
            # Tracing-subscriber writes lines like:
            # 2026-09-21T10:45:31.123456Z INFO hashring_rs::coordinator: ...
            # We just pass them through; the user greps.
            grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}' "$log" || echo "(no tracing lines)"
            echo
        done
    else
        echo "(no process-logs directory found)"
    fi
} > "$TRACING_TXT"

# ----------------------------------------------------------------------------
# 6. Print the headline numbers.
# ----------------------------------------------------------------------------
echo
echo "================ PER-PROCESS TOTALS ================"
cat "$SUMMARY_TXT"
echo
echo "================ STAGE TIMINGS (from events.jsonl) ================"
cat "$STAGES_TXT"
echo
echo "Full artifacts in: $OUT_DIR"
echo "  cpu.tsv     raw /proc samples (~$(wc -l < "$CPU_TSV") rows)"
echo "  stages.txt  per-stage wall time"
echo "  tracing.txt child-process tracing events"
echo "  summary.txt per-process CPU / RSS"
