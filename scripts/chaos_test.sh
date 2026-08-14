#!/usr/bin/env bash
# Chaos test: this is the point of stage 2. Kills the Raft leader mid-write and
# asserts a surviving node takes over and no acknowledged write is lost.
#
# Run from the project root: ./scripts/chaos_test.sh
set -uo pipefail

CP=build
HOST=127.0.0.1
BASE_CLIENT_PORT=${BASE_CLIENT_PORT:-17501}
BASE_RAFT_PORT=${BASE_RAFT_PORT:-17601}
KEYS=${KEYS:-300}
VALUE_SIZE=${VALUE_SIZE:-64}
DATA_DIR=/tmp/kvchaos
NODE_IDS="1 2 3"

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# Plain indexed arrays, not associative (declare -A): node ids are 1/2/3, so a
# regular array works fine, and macOS ships bash 3.2, which has no declare -A.
PID=() CLIENT_PORT=() RAFT_PORT=() LOG=()
for id in $NODE_IDS; do
    CLIENT_PORT[$id]=$((BASE_CLIENT_PORT + id - 1))
    RAFT_PORT[$id]=$((BASE_RAFT_PORT + id - 1))
    LOG[$id]="$DATA_DIR/n$id.log"
done

peers_for() {
    local self=$1 out=""
    for id in $NODE_IDS; do
        [ "$id" = "$self" ] && continue
        out="${out}${id}=$HOST:${RAFT_PORT[$id]},"
    done
    echo "${out%,}"
}

all_client_addrs() {
    local out=""
    for id in $NODE_IDS; do
        out="$out $HOST:${CLIENT_PORT[$id]}"
    done
    echo "$out"
}

start_cluster() {
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    for id in $NODE_IDS; do
        java -cp "$CP" kvstore.Main \
            --port "${CLIENT_PORT[$id]}" --data "$DATA_DIR/n$id/kvstore.wal" \
            --id "$id" --raft-port "${RAFT_PORT[$id]}" --peers "$(peers_for "$id")" \
            >"${LOG[$id]}" 2>&1 &
        PID[$id]=$!
    done
    for id in $NODE_IDS; do
        for _ in $(seq 1 100); do
            grep -q READY "${LOG[$id]}" 2>/dev/null && break
            if ! kill -0 "${PID[$id]}" 2>/dev/null; then
                echo "  node $id died on startup:"; sed 's/^/    /' "${LOG[$id]}"; return 1
            fi
            sleep 0.1
        done
    done
}

# SIGKILL: same reasoning as crash_test.sh's kill_server_hard - no shutdown
# hook, no chance to hand off leadership gracefully. A clean stop would prove
# nothing about whether the cluster can survive an actual failure.
kill_node_hard() {
    # Reap it immediately, not just eventually in stop_cluster - otherwise bash
    # notices the job died later (e.g. during the writer's `wait` below) and
    # prints an unsolicited "Killed: 9" line instead of staying quiet.
    kill -9 "${PID[$1]}" 2>/dev/null
    wait "${PID[$1]}" 2>/dev/null
}

stop_cluster() {
    for id in $NODE_IDS; do
        kill -9 "${PID[$id]}" 2>/dev/null
    done
    for id in $NODE_IDS; do
        wait "${PID[$id]}" 2>/dev/null
    done
}

# First node whose own log shows it won an election - the sole leader, since
# only the winner ever prints this line, followers only log the votes they cast.
current_leader() {
    for id in $NODE_IDS; do
        grep -q "elected leader" "${LOG[$id]}" 2>/dev/null && { echo "$id"; return 0; }
    done
    return 1
}

wait_for_leader() {
    for _ in $(seq 1 100); do
        local leader
        if leader=$(current_leader); then echo "$leader"; return 0; fi
        sleep 0.1
    done
    return 1
}

echo "=============================================="
echo " Chaos test: kill the leader mid-write"
echo " keys=$KEYS value=${VALUE_SIZE}B"
echo "=============================================="

echo
echo "[1] Kill the leader mid-write; assert failover and zero data loss"
start_cluster || exit 1
LEADER=$(wait_for_leader) || { echo "  no leader elected"; fail "initial election"; stop_cluster; exit 1; }
echo "  initial leader: node $LEADER"

# Background writer so the leader can be killed while it's mid-flight. It
# doesn't need to be told a failover happened - ClusterWorkload retries
# against the rest of the cluster on any failure and keeps going.
java -cp "$CP" kvstore.tools.ClusterWorkload write "$KEYS" "$VALUE_SIZE" $(all_client_addrs) \
    >"$DATA_DIR/writer.log" 2>&1 &
WRITER_PID=$!

sleep 0.3 # let a handful of writes land before pulling the rug out
kill_node_hard "$LEADER"
echo "  SIGKILLed leader node $LEADER mid-write"

if wait "$WRITER_PID"; then
    pass "writer finished all $KEYS writes despite the leader dying mid-run"
else
    echo "  writer output:"; sed 's/^/    /' "$DATA_DIR/writer.log"
    fail "writer did not complete cleanly"
fi

SURVIVOR_BECAME_LEADER=0
for id in $NODE_IDS; do
    [ "$id" = "$LEADER" ] && continue
    grep -q "elected leader" "${LOG[$id]}" && SURVIVOR_BECAME_LEADER=1
done
[ "$SURVIVOR_BECAME_LEADER" -eq 1 ] \
    && pass "a surviving node was elected leader after the kill" \
    || fail "no surviving node ever became leader"

if java -cp "$CP" kvstore.tools.ClusterWorkload verify "$KEYS" "$VALUE_SIZE" $(all_client_addrs) | sed 's/^/  /'; then
    pass "every acknowledged write is present after the failover"
else
    fail "data loss: an acknowledged write went missing after failover"
fi

stop_cluster

echo
echo "=============================================="
echo " $PASS passed, $FAIL failed"
echo "=============================================="
[ "$FAIL" -eq 0 ]
