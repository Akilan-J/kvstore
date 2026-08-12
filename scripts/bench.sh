#!/usr/bin/env bash
# Runs the benchmark matrix against a fresh node for each configuration.
# Usage: ./scripts/bench.sh [clients] [opsPerClient]
set -uo pipefail
cd "$(dirname "$0")/.."

CLIENTS=${1:-8}
OPS=${2:-3000}
PORT=${PORT:-7401}
DIR=/tmp/kvbench

run() {
    local sync=$1 mode=$2 ops=$3
    rm -rf "$DIR"; mkdir -p "$DIR"
    java -cp build kvstore.Main --port "$PORT" --data "$DIR/bench.wal" --sync "$sync" >"$DIR/server.log" 2>&1 &
    local pid=$!
    for _ in $(seq 1 100); do
        grep -q READY "$DIR/server.log" 2>/dev/null && break
        sleep 0.1
    done
    echo "--- mode=$mode sync=$sync ---"
    java -cp build kvstore.tools.Bench 127.0.0.1 "$PORT" "$mode" "$CLIENTS" "$ops" 64 \
        | grep -E 'throughput|latency'
    kill -TERM $pid 2>/dev/null
    wait $pid 2>/dev/null
}

echo "clients=$CLIENTS  value=64B  cpus=$(nproc)"
run every write "$OPS"
run never write "$OPS"
run every read  $((OPS * 2))
run every mixed $((OPS * 2))
