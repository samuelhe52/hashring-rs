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
#   ./scripts/flamegraph.sh                      # experiments/experiment_cmach.yaml
#   ./scripts/flamegraph.sh performance           # same file, mode from the argument
#
#   env CONFIG=examples/experiment.yaml ./scripts/flamegraph.sh
#   env BUILD=1 FREQ=199 ./scripts/flamegraph.sh # force a rebuild, sample harder
#   env BINARY=/tmp/other OUT_DIR=/tmp/fg ./scripts/flamegraph.sh
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
#   * The profiled binary is built with -C force-frame-pointers=yes into its own
#     target directory, and the call graph is read with fp. This is the whole
#     difference between a usable profile and an unusable one, so it is not
#     optional. Measured on this host with rustc 1.93.0, on a 30-line busy loop,
#     as the share of samples carrying at least two Rust frames:
#
#         build                     fp        dwarf,8192
#         default rustc -O         0.0%          1.1%
#         force-frame-pointers    94.5%         93.8%
#         force-unwind-tables       n/a          0.0%
#
#     A default release build cannot be unwound past the libc boundary by either
#     method, so a capture of one is comm -> libc -> kernel and nothing else:
#     91.5% of samples carried no Rust frame at all, and the profile could only
#     be read by symbol self time. force-unwind-tables does not help; .eh_frame
#     already covers 551 of 578 functions in that repro. Use this script's own
#     build, or any build with frame pointers, and point BINARY at it.
#   * fp also removes the DWARF sampling problem. perf's per-CPU ring buffer is
#     bounded by /proc/sys/kernel/perf_event_mlock_kb (516 KB here) shared across
#     every CPU, which leaves ~22 KB per CPU on a 24-core host. A 64000-byte
#     DWARF stack makes each sample ~56 KB, so not even one sample fits and the
#     kernel drops records wholesale: a capture with 64000 lost 12.33% of samples
#     over 385 chunks, while 8192 lost 0.4% over 8 chunks for the same run
#     length. CALL_GRAPH=dwarf,<DWARF_STACK> selects that path.
#   * Frame pointers cost a little throughput, so the build lives in its own
#     target directory and never touches target/release. Measure with one build,
#     profile with the other; do not ship the frame-pointer binary.
#   * The build runs before perf starts, never under it. Wrapping this in `cargo
#     run` would put cargo and any rustc invocation into the profile alongside
#     the experiment.
#   * Every capture reports the share of samples that carry a project frame.
#     Below a few percent the call graph is truncated and the SVG is a wall of
#     comm -> libc -> kernel; the number is the check, not the graph's shape.
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
CONFIG="${CONFIG:-experiments/experiment_cmach.yaml}"
# Separate target directory: frame pointers are a profiling build, not the one to
# benchmark or ship. TARGET_DIR only matters when building, and must match BINARY.
TARGET_DIR="${TARGET_DIR:-target/fp}"
BINARY="${BINARY:-$TARGET_DIR/release/hashring-rs}"
BUILD="${BUILD:-auto}"
FREQ="${FREQ:-99}"
CALL_GRAPH="${CALL_GRAPH:-fp}"
DWARF_STACK="${DWARF_STACK:-8192}"

case "$MODE" in
  "" | correctness | performance | availability) ;;
  *)
    echo "usage: $0 [correctness|performance|availability]" >&2
    exit 2
    ;;
esac

case "$CALL_GRAPH" in
  fp | dwarf) ;;
  *)
    echo "error: CALL_GRAPH must be 'fp' or 'dwarf'." >&2
    exit 2
    ;;
esac
if [[ "$CALL_GRAPH" == "fp" ]]; then
  CALL_GRAPH_ARG=fp
else
  CALL_GRAPH_ARG="dwarf,$DWARF_STACK"
fi

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

# perf needs the frame-pointer build; see the note at the top of this file.
build_binary() {
  if ! command -v cargo >/dev/null 2>&1; then
    echo "error: 'cargo' not found, and '$BINARY' has to be built." >&2
    exit 1
  fi
  echo "==> building $BINARY with -C force-frame-pointers=yes"
  echo "    (perf cannot unwind this code without frame pointers; see the header)"
  RUSTFLAGS="-C force-frame-pointers=yes${RUSTFLAGS:+ $RUSTFLAGS}" \
    cargo build --release --target-dir "$TARGET_DIR"
  echo
}

case "$BUILD" in
  # cargo build is a no-op when the frame-pointer target is current, so auto
  # always asks: building only when the binary is missing would profile a stale
  # one after any source change. A BINARY from elsewhere is used as given.
  auto)
    if [[ "$BINARY" == "$TARGET_DIR/release/hashring-rs" ]]; then
      # Without cargo an existing build is still usable; with neither,
      # build_binary reports the missing cargo.
      if command -v cargo >/dev/null 2>&1 || [[ ! -x "$BINARY" ]]; then
        build_binary
      else
        echo "==> WARNING: 'cargo' not found; profiling '$BINARY' as built." >&2
      fi
    fi
    ;;
  1 | yes | true) build_binary ;;
  0 | no | false) ;;
  *)
    echo "error: BUILD must be auto, 1 or 0." >&2
    exit 2
    ;;
esac

if [[ ! -x "$BINARY" ]]; then
  echo "error: binary '$BINARY' not found or not executable." >&2
  echo "       build it with:" >&2
  echo "         RUSTFLAGS='-C force-frame-pointers=yes' cargo build --release --target-dir $TARGET_DIR" >&2
  case "$BUILD" in
    0 | no | false) echo "       or drop BUILD=0 and let this script build it." >&2 ;;
  esac
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
echo "==> call graph  : $CALL_GRAPH_ARG"
echo "==> frequency   : ${FREQ} Hz"
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
perf record -o "$PERF_DATA" -F "$FREQ" --call-graph "$CALL_GRAPH_ARG" -- \
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

# Truncated stacks look like a deep, plausible graph, because kernel frames still
# unwind. This share is the honest check: a real profile puts project frames
# almost everywhere, a truncated one puts them nowhere.
# A folded line is `comm;frame;...;leaf weight`. Symbols can contain spaces
# (`<A as B>::f`), so the weight is the text after the last space, and the leaf
# is the last field once that is stripped; the first field is the process name,
# not a frame.
RESOLVED="$(awk -F';' '
  { weight = $0; sub(/.* /, "", weight); weight += 0
    leaf = $NF; sub(/ [^ ]*$/, "", leaf)
    project = (NF > 1 && leaf ~ /^<?hashring[_-]/)
    for (i = 2; i < NF; i++) if ($i ~ /^<?hashring[_-]/) project = 1
    total += weight; if (project) resolved += weight }
  END { if (total > 0) printf "%.1f", 100 * resolved / total; else print "0.0" }
' "$FOLDED")"
printf '    %s%% of samples carry a hashring frame\n' "$RESOLVED"
echo

if awk -v share="$RESOLVED" 'BEGIN { exit !(share < 5) }'; then
  echo "==> WARNING: the call graph looks truncated." >&2
  echo "    Only $RESOLVED% of samples reached this project's code, so the graph" >&2
  echo "    is comm -> libc -> kernel and cannot be read. Check that BINARY was" >&2
  echo "    built with -C force-frame-pointers=yes, and that CALL_GRAPH matches it." >&2
  echo >&2
fi

echo "==> rendering..."
inferno-flamegraph \
  --title "hashring-rs $LABEL" \
  --subtitle "perf cpu/cycles @${FREQ}Hz, --call-graph $CALL_GRAPH_ARG, $(basename "$CONFIG")" \
  --countname cycles \
  "$FOLDED" > "$SVG"

echo "==> wrote:"
for file in "$SVG" "$FOLDED" "$PERF_DATA"; do
  printf '    %10s  %s\n' "$(du -h "$file" | cut -f1)" "$file"
done
echo
echo "    Check perf-record.log for sample loss before trusting the graph."
echo "    Delete perf.data once the SVG is all you need; it dominates the size."
