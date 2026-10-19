#!/usr/bin/env bash
# Interleaved A/B of baseline (main) vs branch (ack-state-lock) on the
# correctness run's initial load. Run from the hashring-rs repo root.
set -uo pipefail
S="${S:?set S to a directory holding bin/baseline, bin/branch and extract.py}"
REPS="${REPS:-5}"
CONCS="${CONCS:-64 256}"
OUT="$S/ab-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
TSV="$OUT/ab.tsv"
printf "conc\trep\tvariant\tput_s\tresv_rej\trpc_retry\towner_acq\towner_hold_ns\towner_wait_ns\tfoll_acq\tfoll_hold_ns\tfoll_wait_ns\tack_acq\tack_hold_ns\tack_wait_ns\tackc_acq\tackc_hold_ns\tackc_wait_ns\n" > "$TSV"
for c in $CONCS; do
  for r in $(seq 1 "$REPS"); do
    # ABBA: alternate which variant goes first so drift does not favor one.
    if (( r % 2 )); then order="baseline branch"; else order="branch baseline"; fi
    for v in $order; do
      d="$OUT/$c-$r-$v"
      HASHRING_PROFILE_WRITES=1 "$S/bin/$v" experiment \
        --mode correctness --concurrency "$c" --output "$d" > "$d.log" 2>&1
      status=$?
      if [[ -f "$d/events.jsonl" ]]; then
        row=$(python3 -I "$S/extract.py" "$d/events.jsonl")
        printf "%s\t%s\t%s\t%s\n" "$c" "$r" "$v" "$row" >> "$TSV"
        echo "conc=$c rep=$r $v exit=$status put_s=$(echo "$row" | cut -f1)"
      else
        echo "conc=$c rep=$r $v exit=$status NO EVENTS (see $d.log)"
      fi
    done
  done
done
echo "raw: $TSV"
