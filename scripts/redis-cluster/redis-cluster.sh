#!/usr/bin/env bash
# Start, stop and inspect a local Redis Cluster. No container runtime needed:
# every node is a redis-server process with its own port, directory and
# cluster-config file.
#
#   redis-cluster.sh start <dir> <masters> [replicas] [base_port]
#   redis-cluster.sh stop  <dir>
#   redis-cluster.sh status <dir>
#
# Binaries come from REDIS_BIN_DIR (default: the directory of `redis-server` on
# PATH). Every node logs to <dir>/<port>/redis.log and keeps its PID in
# <dir>/<port>/redis.pid.
set -euo pipefail

REDIS_BIN_DIR="${REDIS_BIN_DIR:-$(dirname "$(command -v redis-server)")}"
REDIS_SERVER="$REDIS_BIN_DIR/redis-server"
REDIS_CLI="$REDIS_BIN_DIR/redis-cli"

usage() {
    echo "usage: $0 {start|stop|status} <dir> [masters] [replicas] [base_port]" >&2
    exit 2
}

cmd="${1:-}"
dir="${2:-}"
[ -n "$cmd" ] && [ -n "$dir" ] || usage
shift 2

case "$cmd" in
start)
    masters="${1:-3}"
    replicas="${2:-0}"
    base_port="${3:-7001}"
    [ -x "$REDIS_SERVER" ] || { echo "no redis-server in $REDIS_BIN_DIR" >&2; exit 1; }
    [ -x "$REDIS_CLI" ] || { echo "no redis-cli in $REDIS_BIN_DIR" >&2; exit 1; }
    mkdir -p "$dir"
    total=$((masters * (replicas + 1)))
    echo "starting $total redis node(s), $masters master(s), $replicas replica(s) per master"
    for i in $(seq 0 $((total - 1))); do
        port=$((base_port + i))
        node_dir="$dir/$port"
        mkdir -p "$node_dir"
        cat > "$node_dir/redis.conf" <<-EOF
		port $port
		bind 127.0.0.1
		protected-mode no
		cluster-enabled yes
		cluster-config-file nodes.conf
		cluster-node-timeout 5000
		appendonly no
		save ""
		daemonize no
		dir $node_dir
		logfile $node_dir/redis.log
		maxmemory 0
		latency-monitor-threshold 0
		EOF
        # The log file in the config takes the output, and the standard streams
        # are closed so that a caller piping this script's output is not held
        # open by the servers.
        "$REDIS_SERVER" "$node_dir/redis.conf" </dev/null >/dev/null 2>&1 &
        echo $! > "$node_dir/redis.pid"
    done
    # Wait for every node to answer PING.
    for i in $(seq 0 $((total - 1))); do
        port=$((base_port + i))
        for _ in $(seq 1 100); do
            if "$REDIS_CLI" -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then break; fi
            sleep 0.1
        done
        "$REDIS_CLI" -h 127.0.0.1 -p "$port" ping >/dev/null || { echo "node $port did not start" >&2; exit 1; }
    done
    endpoints=""
    for i in $(seq 0 $((total - 1))); do
        port=$((base_port + i))
        endpoints="$endpoints 127.0.0.1:$port"
    done
    # shellcheck disable=SC2086
    "$REDIS_CLI" --cluster create $endpoints --cluster-replicas "$replicas" --cluster-yes > "$dir/cluster-create.log" 2>&1
    for _ in $(seq 1 100); do
        if "$REDIS_CLI" --cluster check 127.0.0.1:"$base_port" >/dev/null 2>&1; then break; fi
        sleep 0.2
    done
    # The check can pass while a node's own gossip view is still settling, and
    # a client that arrives then is answered with CLUSTERDOWN. Wait for every
    # node to report an ok state.
    for i in $(seq 0 $((total - 1))); do
        port=$((base_port + i))
        for _ in $(seq 1 100); do
            if "$REDIS_CLI" -h 127.0.0.1 -p "$port" cluster info 2>/dev/null | grep -q "^cluster_state:ok"; then break; fi
            sleep 0.1
        done
        "$REDIS_CLI" -h 127.0.0.1 -p "$port" cluster info 2>/dev/null | grep -q "^cluster_state:ok" || {
            echo "node $port never reached an ok cluster state" >&2
            exit 1
        }
    done
    "$REDIS_CLI" --cluster check 127.0.0.1:"$base_port" | tail -3
    for pid_file in "$dir"/*/redis.pid; do
        [ -f "$pid_file" ] || continue
        pid=$(cat "$pid_file")
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "redis node $pid is not running" >&2
            tail -20 "$(dirname "$pid_file")/redis.log" >&2
            exit 1
        fi
    done
    ;;
stop)
    if [ -d "$dir" ]; then
        for pid_file in "$dir"/*/redis.pid; do
            [ -f "$pid_file" ] || continue
            pid=$(cat "$pid_file")
            kill "$pid" 2>/dev/null || true
        done
        sleep 0.5
        for pid_file in "$dir"/*/redis.pid; do
            [ -f "$pid_file" ] || continue
            pid=$(cat "$pid_file")
            kill -9 "$pid" 2>/dev/null || true
            rm -f "$pid_file"
        done
    fi
    echo "stopped"
    ;;
status)
    for pid_file in "$dir"/*/redis.pid; do
        [ -f "$pid_file" ] || continue
        pid=$(cat "$pid_file")
        port=$(basename "$(dirname "$pid_file")")
        role=$("$REDIS_CLI" -h 127.0.0.1 -p "$port" role 2>/dev/null | head -1)
        echo "$port pid=$pid $role"
    done
    ;;
*)
    usage
    ;;
esac
