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
#   ./scripts/flamegraph.sh                      # examples/experiment_cmach.yaml
#   ./scripts/flamegraph.sh performance           # same file, mode from the argument
#
#   env CONFIG=examples/experiment.yaml ./scripts/flamegraph.sh
#   env BINARY=target/release/hashring-rs OUT_DIR=/tmp/fg ./scripts/flamegraph.sh
#
# Settings
# --------
#   The run's settings come from the YAML file given by CONFIG, so the mode,
#   node count, key count and acknowledgement policy of one profile are exactly
#   the ones that experiment produces elsewhere. The argument, when given, is
#   passed as --mode and therefore overrides the file's `mode:` key; without it
#   the file decides. --output is always passed, and overrides `output` in the
#   file, because every capture needs its own directory.
#
# Output
# ------
#   results/flamegraph-<mode>-<timestamp>/
#     flamegraph.svg          the rendered graph
#     folded.txt              folded stacks, the input to the renderer
#     perf.data               raw samples; large, safe to delete afterwards
#     perf-record.log         perf's own report, including any sample loss
#     run/                    the experiment's own output directory, manifest included
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
#     perf-profile.sh, so the script runs regardless of working-tree state. A
#     config file that sets `require_clean_source: true` still applies it. The
#     experiment records source_reproducible in its manifest either way.

set -euo pipefail

# Empty unless the caller pins a mode; otherwise the config file's `mode:` key
# applies, and this only names the output directory and the graph title.
MODE="${1:-}"
CONFIG="${CONFIG:-examples/experiment_cmach.yaml}"
BINARY="${BINARY:-target/release/hashring-rs}"
FREQ="${FREQ:-99}"
DWARF_STACK="${DWARF_STACK:-8192}"

case "$MODE" in
  "" | correctness | performance | availability) ;;
  *)
    echo "usage: $0 [correctness|performance|availability]" >&2
    exit 2
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

if [[ ! -f "$CONFIG" ]]; then
  echo "error: config '$CONFIG' not found." >&2
  echo "       set CONFIG=..., for example CONFIG=examples/experiment.yaml." >&2
  exit 1
fi

# Label only: the experiment resolves the real settings, and run/manifest.json
# records what it resolved. This reads the file's top-level `mode:` and tolerates
# a quoted value; `correctness` is the experiment's own default when neither the
# file nor an argument sets one.
LABEL="${MODE:-$(sed -n $'s/^mode:[[:space:]]*["\']\\{0,1\\}\\([a-z-]\\{1,\\}\\).*/\\1/p' "$CONFIG" | head -n 1)}"
LABEL="${LABEL:-correctness}"

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

OUT_DIR="${OUT_DIR:-results/flamegraph-$LABEL-$(date +%Y%m%d-%H%M%S)}"
if [[ -e "$OUT_DIR" ]]; then
  echo "error: '$OUT_DIR' already exists; refusing to overwrite." >&2
  exit 1
fi
mkdir -p "$OUT_DIR"

PERF_DATA="$OUT_DIR/perf.data"
PERF_LOG="$OUT_DIR/perf-record.log"
FOLDED="$OUT_DIR/folded.txt"
SVG="$OUT_DIR/flamegraph.svg"

echo "==> config      : $CONFIG"
echo "==> mode        : $LABEL${MODE:+ (--mode overrides the file)}"
echo "==> binary      : $BINARY"
echo "==> frequency   : ${FREQ} Hz"
echo "==> dwarf stack : ${DWARF_STACK} bytes"
echo "==> output dir  : $OUT_DIR"
echo

# --mode only when the caller gave one: passing it unconditionally would
# override the file's mode and the run would not be the configured one.
EXPERIMENT=(experiment --config "$CONFIG" --output "$OUT_DIR/run")
[[ -z "$MODE" ]] || EXPERIMENT+=(--mode "$MODE")

echo "==> recording..."
# perf exits with the experiment's status, and the experiment's own stderr lands
# in the log, so a failure must still print it: a stale binary rejects --config,
# and nothing else in this script would say so.
status=0
perf record -o "$PERF_DATA" -F "$FREQ" --call-graph "dwarf,$DWARF_STACK" -- \
  "$BINARY" "${EXPERIMENT[@]}" 2>"$PERF_LOG" || status=$?

echo "==> perf report:"
sed -n 's/^/    /p' "$PERF_LOG"
echo

if [[ "$status" -ne 0 ]]; then
  echo "error: perf record exited $status; nothing was profiled." >&2
  exit "$status"
fi

# The experiment resolves flags over file keys and defaults for the rest, so its
# manifest, not this script, is the record of what actually ran. Read the config
# block only: the manifest also inlines a git diff that can hold any text.
if [[ -f "$OUT_DIR/run/manifest.json" ]]; then
  echo "==> resolved:"
  sed -n '/^  "config": {/,/^  }/p' "$OUT_DIR/run/manifest.json" \
    | grep -o '"\(mode\|node_count\|key_count\|worker_threads\)": [^,]*' \
    | sed -n 's/^/    /p' || true
  echo
fi

echo "==> collapsing stacks..."
perf script -i "$PERF_DATA" 2>/dev/null | inferno-collapse-perf > "$FOLDED"
echo "    $(wc -l < "$FOLDED") folded stacks"
echo

echo "==> rendering..."
inferno-flamegraph \
  --title "hashring-rs $LABEL" \
  --subtitle "perf cpu/cycles @${FREQ}Hz, --call-graph dwarf,${DWARF_STACK}, $(basename "$CONFIG")" \
  --countname cycles \
  "$FOLDED" > "$SVG"

echo "==> wrote:"
for file in "$SVG" "$FOLDED" "$PERF_DATA"; do
  printf '    %10s  %s\n' "$(du -h "$file" | cut -f1)" "$file"
done
echo
echo "    Check perf-record.log for sample loss before trusting the graph."
echo "    Delete perf.data once the SVG is all you need; it dominates the size."
