#!/usr/bin/env bash
# flamegraph.sh — capture and render a flame graph for a hashring-rs experiment.
#
# What it measures
# ----------------
#   * CPU cycles for every process in the experiment (perf's inherit follows the
#     spawned coordinator and data nodes), folded into a call graph and rendered
#     as an SVG flame graph.
#
# Usage
# -----
#   ./scripts/flamegraph.sh                      # correctness, 20k keys
#   ./scripts/flamegraph.sh performance           # performance, 1M keys
#
#   env BINARY=target/release/hashring-rs OUT_DIR=/tmp/fg ./scripts/flamegraph.sh
#
# Output
# ------
#   results/flamegraph-<mode>-<timestamp>/
#     flamegraph.svg          the rendered graph
#     folded.txt              folded stacks, the input to the renderer
#     perf.data               raw samples; large, safe to delete afterwards
#     perf-record.log         perf's own report, including any sample loss
#     run/                    the experiment's own output directory
#
# Notes
# -----
#   * The DWARF stack size is the setting that decides whether this profile is
#     usable. perf's per-CPU ring buffer is bounded by
#     /proc/sys/kernel/perf_event_mlock_kb (516 KB here) shared across every CPU,
#     which leaves ~22 KB per CPU on a 24-core host. A 64000-byte stack makes
#     each sample ~56 KB, so not even one sample fits and the kernel drops
#     records wholesale: a capture with 64000 lost 12.33% of samples over 385
#     chunks, while 8192 lost 0.4% over 8 chunks for the same run length.
#   * Frame pointers are not forced in the release profile and Rust omits them
#     by default, so --call-graph fp yields truncated Rust stacks. Keep dwarf.
#   * Build first and profile the binary directly. Wrapping this in `cargo run`
#     puts cargo and any rustc invocation into the profile alongside the
#     experiment.
#   * The rendered graph is inverted: "all" sits at the bottom and leaves at the
#     top, so a frame's children are at y-16, not y+16. Reading the SVG as
#     though it were a normal flame graph yields self times that sum to several
#     times the root.
#   * --require-clean-source is intentionally not passed, matching
#     perf-profile.sh, so the script runs regardless of working-tree state. The
#     experiment still records source_reproducible in its manifest.

set -euo pipefail

MODE="${1:-correctness}"
BINARY="${BINARY:-target/release/hashring-rs}"
FREQ="${FREQ:-99}"
DWARF_STACK="${DWARF_STACK:-8192}"

if [[ "$MODE" != "correctness" && "$MODE" != "performance" ]]; then
  echo "usage: $0 [correctness|performance]" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# inferno installs its binaries into ~/.cargo/bin, which is not on PATH unless a
# shell has sourced ~/.cargo/env -- a file that is absent on some hosts.
export PATH="$HOME/.cargo/bin:$PATH"

missing=0
for tool in perf inferno-collapse-perf inferno-flamegraph; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: '$tool' not found." >&2
    case "$tool" in
      inferno-*) echo "       run 'cargo install inferno'" >&2 ;;
      perf)      echo "       install perf, and check /proc/sys/kernel/perf_event_paranoid" >&2 ;;
    esac
    missing=1
  fi
done
[[ "$missing" -eq 0 ]] || exit 1

if [[ ! -x "$BINARY" ]]; then
  echo "error: binary '$BINARY' not found or not executable." >&2
  echo "       run 'cargo build --release' first." >&2
  exit 1
fi

OUT_DIR="${OUT_DIR:-results/flamegraph-$MODE-$(date +%Y%m%d-%H%M%S)}"
if [[ -e "$OUT_DIR" ]]; then
  echo "error: '$OUT_DIR' already exists; refusing to overwrite." >&2
  exit 1
fi
mkdir -p "$OUT_DIR"

PERF_DATA="$OUT_DIR/perf.data"
PERF_LOG="$OUT_DIR/perf-record.log"
FOLDED="$OUT_DIR/folded.txt"
SVG="$OUT_DIR/flamegraph.svg"

echo "==> mode        : $MODE"
echo "==> binary      : $BINARY"
echo "==> frequency   : ${FREQ} Hz"
echo "==> dwarf stack : ${DWARF_STACK} bytes"
echo "==> output dir  : $OUT_DIR"
echo

echo "==> recording..."
perf record -o "$PERF_DATA" -F "$FREQ" --call-graph "dwarf,$DWARF_STACK" -- \
  "$BINARY" experiment --mode "$MODE" --output "$OUT_DIR/run" 2>"$PERF_LOG"

echo "==> perf report:"
sed -n 's/^/    /p' "$PERF_LOG"
echo

echo "==> collapsing stacks..."
perf script -i "$PERF_DATA" 2>/dev/null | inferno-collapse-perf > "$FOLDED"
echo "    $(wc -l < "$FOLDED") folded stacks"
echo

echo "==> rendering..."
inferno-flamegraph \
  --title "hashring-rs $MODE" \
  --subtitle "perf cpu/cycles @${FREQ}Hz, --call-graph dwarf,${DWARF_STACK}" \
  --countname cycles \
  "$FOLDED" > "$SVG"

echo "==> wrote:"
for file in "$SVG" "$FOLDED" "$PERF_DATA"; do
  printf '    %10s  %s\n' "$(du -h "$file" | cut -f1)" "$file"
done
echo
echo "    Check perf-record.log for sample loss before trusting the graph."
echo "    Delete perf.data once the SVG is all you need; it dominates the size."
