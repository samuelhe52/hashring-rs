#!/usr/bin/env bash
# Start, stop and inspect a hashring-rs cluster with one or more Redis
# gateways in front of it.
#
#   hashring-cluster.sh start <dir> <nodes> <rf> [gateways] [base_node_port] [base_gateway_port] [binary]
#   hashring-cluster.sh stop  <dir>
#   hashring-cluster.sh status <dir>
#
# The coordinator listens on 127.0.0.1:50050, node i on 127.0.0.1:(base_node_port+i-1)
# and gateway g on 127.0.0.1:(base_gateway_port+g-1). Every process logs to
# <dir>/<name>.log and keeps its PID in <dir>/<name>.pid. The resolved worker
# count and budgets are written to <dir>/settings.
#
# The node budgets below are set for a benchmark rather than for a default
# deployment. A node keeps one deduplication receipt per write for the receipt
# retention period, so the default 128 MiB store fills after a few seconds of
# sustained writes and the node then rejects writes. HASHRING_MAX_DEDUP_BYTES
# raises that budget, so the numbers measure the data path and not the
# receipt store. HASHRING_GATEWAY_DEADLINE_MS raises the gateway deadline for
# the same reason: Redis has no deadline, and a saturated gateway should not
# answer TRYAGAIN while it is still making progress.
set -euo pipefail

BIN="${HASHRING_BIN:-}"
cmd="${1:-}"
dir="${2:-}"
[ -n "$cmd" ] && [ -n "$dir" ] || { echo "usage: $0 {start|stop|status} <dir> ..." >&2; exit 2; }
shift 2

case "$cmd" in
start)
    nodes="${1:-3}"
    rf="${2:-1}"
    gateways="${3:-1}"
    base_node_port="${4:-50051}"
    base_gateway_port="${5:-6399}"
    if [ -z "$BIN" ]; then
        BIN="${HASHRING_BIN:-$PWD/target/release/hashring-rs}"
    fi
    [ -x "$BIN" ] || { echo "no hashring-rs binary at $BIN" >&2; exit 1; }
    cpus=$(nproc)
    workers=$((cpus / (nodes + 2)))
    [ "$workers" -lt 2 ] && workers=2
    max_dedup_bytes="${HASHRING_MAX_DEDUP_BYTES:-2147483648}"
    max_pending_bytes="${HASHRING_MAX_PENDING_REPLICATION_BYTES:-536870912}"
    gateway_deadline_ms="${HASHRING_GATEWAY_DEADLINE_MS:-60000}"
    # How many HTTP/2 connections the gateway opens to each data node, and
    # how many mutations one replication RPC carries. Both default to what the
    # binary defaults to, so an unset environment measures the defaults.
    connection_pool="${HASHRING_GATEWAY_CONNECTION_POOL:-1}"
    batch_max_entries="${HASHRING_REPLICATION_BATCH_MAX_ENTRIES:-64}"
    batch_linger_ms="${HASHRING_REPLICATION_BATCH_LINGER_MS:-2}"
    mkdir -p "$dir"
    # The resolved settings, for the harness to record with its results.
    cat > "$dir/settings" <<SETTINGS
nodes=$nodes
rf=$rf
gateways=$gateways
worker_threads=$workers
max_dedup_bytes=$max_dedup_bytes
max_pending_replication_bytes=$max_pending_bytes
gateway_deadline_ms=$gateway_deadline_ms
SETTINGS
    echo "starting coordinator, $nodes node(s), rf=$rf, $gateways gateway(s), $workers worker(s) each"
    echo "gateway connection pool=$connection_pool, replication batch=$batch_max_entries entries, linger=${batch_linger_ms}ms"
    members=""
    for i in $(seq 1 "$nodes"); do
        port=$((base_node_port + i - 1))
        members="$members --member node-$i=http://127.0.0.1:$port"
    done
    # shellcheck disable=SC2086
    "$BIN" coordinator --state "$dir/coordinator.redb" --listen 127.0.0.1:50050 \
        $members --desired-replication-factor "$rf" --worker-threads 2 \
        > "$dir/coordinator.log" 2>&1 &
    echo $! > "$dir/coordinator.pid"
    for _ in $(seq 1 100); do
        grep -q "coordinator" "$dir/coordinator.log" 2>/dev/null && break
        sleep 0.1
    done
    sleep 0.3
    for i in $(seq 1 "$nodes"); do
        port=$((base_node_port + i - 1))
        "$BIN" node --id "node-$i" --listen "127.0.0.1:$port" \
            --coordinator http://127.0.0.1:50050 --worker-threads "$workers" \
            --max-dedup-bytes "$max_dedup_bytes" \
            --max-pending-replication-bytes "$max_pending_bytes" \
            --replication-batch-max-entries "$batch_max_entries" \
            --replication-batch-linger-ms "$batch_linger_ms" \
            > "$dir/node-$i.log" 2>&1 &
        echo $! > "$dir/node-$i.pid"
    done
    for g in $(seq 1 "$gateways"); do
        port=$((base_gateway_port + g - 1))
        "$BIN" redis-gateway --listen "127.0.0.1:$port" \
            --coordinator http://127.0.0.1:50050 --worker-threads "$workers" \
            --deadline-ms "$gateway_deadline_ms" \
            --client-connection-pool "$connection_pool" \
            > "$dir/gateway-$g.log" 2>&1 &
        echo $! > "$dir/gateway-$g.pid"
    done
    # Wait for the gateway to answer PING, which means the cluster is serving.
    for g in $(seq 1 "$gateways"); do
        port=$((base_gateway_port + g - 1))
        ready=0
        for _ in $(seq 1 200); do
            if printf 'PING\r\n' | timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port; cat >&3; head -c 7 <&3" 2>/dev/null | grep -q PONG; then
                ready=1
                break
            fi
            sleep 0.2
        done
        [ "$ready" = 1 ] || { echo "gateway $port never answered PING" >&2; tail -20 "$dir/gateway-$g.log" >&2; exit 1; }
    done
    # Every process must still be alive. A node that could not bind its port
    # is a node that is not in the cluster, and a benchmark against it would
    # measure a broken cluster.
    for pid_file in "$dir"/*.pid; do
        [ -f "$pid_file" ] || continue
        pid=$(cat "$pid_file")
        name=$(basename "$pid_file" .pid)
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "$name (pid $pid) is not running" >&2
            tail -20 "$dir/$name.log" >&2
            exit 1
        fi
    done
    echo "cluster ready: gateway(s) on $(seq "$base_gateway_port" $((base_gateway_port + gateways - 1)) | tr '\n' ' ')"
    ;;
stop)
    if [ -d "$dir" ]; then
        for pid_file in "$dir"/*.pid; do
            [ -f "$pid_file" ] || continue
            kill "$(cat "$pid_file")" 2>/dev/null || true
        done
        sleep 0.5
        for pid_file in "$dir"/*.pid; do
            [ -f "$pid_file" ] || continue
            kill -9 "$(cat "$pid_file")" 2>/dev/null || true
            rm -f "$pid_file"
        done
    fi
    echo "stopped"
    ;;
status)
    for pid_file in "$dir"/*.pid; do
        [ -f "$pid_file" ] || continue
        pid=$(cat "$pid_file")
        name=$(basename "$pid_file" .pid)
        if kill -0 "$pid" 2>/dev/null; then state=running; else state=dead; fi
        echo "$name pid=$pid $state"
    done
    ;;
*)
    echo "usage: $0 {start|stop|status} <dir> ..." >&2
    exit 2
    ;;
esac
