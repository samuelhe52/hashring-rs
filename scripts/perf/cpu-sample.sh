#!/bin/bash
# Usage: cpu-sample.sh <out.tsv>   Samples per-process CPU of every hashring-rs process every 0.25 s until killed.
out="$1"; printf 'unix_ms\tpid\trole\tutime\tstime\n' > "$out"
trap 'exit 0' TERM
while true; do
  now=$(python3 -c 'import time;print(int(time.time()*1000))')
  ps -A -o pid=,utime=,stime=,command= | awk -v now="$now" '
    $4 ~ /hashring-rs$/ {
      role=$5; for(i=5;i<=NF;i++) if($i=="--id") role="node:" $(i+1);
      print now "\t" $1 "\t" role "\t" $2 "\t" $3 }' >> "$out"
  sleep 0.25
done
