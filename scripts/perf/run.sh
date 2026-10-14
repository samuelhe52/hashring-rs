#!/bin/bash
# One performance-mode run with per-process CPU sampling and, when
# HASHRING_PROFILE_RPC=1 is set, the RPC latency summary.
# Usage: scripts/perf/run.sh <out_root> <config_name> <rep> [extra experiment flags...]
# Writes <out_root>/<config>/rep<N>/ (the experiment output) plus rep<N>.cpu.txt,
# rep<N>.rpc.txt and the driver's rep<N>.stdout/.stderr. Summarize with collect.py.
# Env passthrough: set PROFILE_WRITES=1 to export HASHRING_PROFILE_WRITES=1 to the run.
# Fixed base: performance mode, 6 nodes, seed 1. Extra flags override (e.g. --keys, --concurrency).
set -u
root="$1"; name="$2"; rep="$3"; shift 3
here="$(cd "$(dirname "$0")" && pwd)"
bin="${BIN:-$here/../../target/release/hashring-rs}"
dir="$root/$name/rep$rep"
mkdir -p "$root/$name"
if [ -e "$dir" ]; then echo "SKIP: $dir exists" >&2; exit 3; fi
[ "${PROFILE_WRITES:-0}" = 1 ] && export HASHRING_PROFILE_WRITES=1
"$here/cpu-sample.sh" "$dir.cpu.tsv" & sp=$!
start=$(date +%s)
"$bin" experiment --mode performance --nodes 6 --seed 1 "$@" --output "$dir" > "$dir.stdout" 2> "$dir.stderr"
rc=$?
kill $sp 2>/dev/null; wait $sp 2>/dev/null
echo "$name rep$rep exit=$rc wall=$(( $(date +%s) - start ))s"
[ $rc -eq 0 ] && python3 "$here/cpu-window.py" "$dir" "$dir.cpu.tsv" > "$dir.cpu.txt" 2>/dev/null
[ $rc -eq 0 ] && grep -q '^rpc_profile ' "$dir.stderr" && python3 "$here/rpc-profile.py" "$dir" > "$dir.rpc.txt"
exit $rc
