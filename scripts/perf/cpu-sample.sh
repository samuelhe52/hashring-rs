#!/bin/bash
# Usage: cpu-sample.sh <out.tsv> [binary]
# Samples per-process CPU every 0.25 s until killed, for every process whose
# executable is <binary> (as the experiment spawns it) or is named hashring-rs.
out="$1"; bin="${2:-}"; printf 'unix_ms\tpid\trole\tutime\tstime\n' > "$out"
trap 'exit 0' TERM
while true; do
  now=$(python3 -c 'import time;print(int(time.time()*1000))')
  ps -A -o pid=,utime=,stime=,command= | awk -v now="$now" -v bin="$bin" '
    ($4 == bin || $4 ~ /hashring-rs$/) {
      role=$5; for(i=5;i<=NF;i++) if($i=="--id") role="node:" $(i+1);
      print now "\t" $1 "\t" role "\t" $2 "\t" $3 }' >> "$out"
  sleep 0.25
done
