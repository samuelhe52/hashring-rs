#!/usr/bin/env bash
# Per-role worker-thread A/B: driver / coordinator / node threads.
set -uo pipefail
cd "$(dirname "$0")"
REPS=${REPS:-6}
declare -A V=( [all2]="2 2 2" [node8]="2 2 8" [coord8]="2 8 2" [driver8]="8 2 2" [all8]="8 8 8" )
ORDER=(all2 node8 coord8 driver8 all8)
mkdir -p runs
for ((r=1; r<=REPS; r++)); do
  for ((i=0; i<${#ORDER[@]}; i++)); do
    v=${ORDER[$(( (i + r) % ${#ORDER[@]} ))]}
    read d c n <<<"${V[$v]}"
    out=runs/$v-$r; rm -rf "$out"
    echo "$(date +%T) rep $r $v (driver=$d coord=$c node=$n)"
    HASHRING_PROFILE_WRITES=1 HASHRING_AB_COORD_THREADS=$c HASHRING_AB_NODE_THREADS=$n \
      ./hashring-ab --worker-threads "$d" experiment --mode correctness --output "$out" \
      > "$out.log" 2>&1 || echo "  FAILED: see $out.log"
  done
done
echo done
