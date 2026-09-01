#!/usr/bin/env bash
# Restart test: proves Raft state is actually durable.
#
# The chaos test kills nodes and leaves them dead, so it never exercises
# recovery. These cases restart nodes and assert they come back with the term,
# vote, and log they had before - which is what stops a restarted node voting
# twice in one term, and what lets a cluster survive losing every node at once.
#
# Run from the project root: ./scripts/restart_test.sh
set -uo pipefail

CP=build
HOST=127.0.0.1
BASE_CLIENT_PORT=${BASE_CLIENT_PORT:-17801}
BASE_RAFT_PORT=${BASE_RAFT_PORT:-17901}
KEYS=${KEYS:-150}
VALUE_SIZE=${VALUE_SIZE:-64}
DATA_DIR=/tmp/kvrestart
NODE_IDS="1 2 3"

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# Plain indexed arrays, not associative: macOS ships bash 3.2, which has no
# declare -A. Same reasoning as chaos_test.sh.
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
    for id in $NODE_IDS; do out="$out $HOST:${CLIENT_PORT[$id]}"; done
    echo "$out"
}

all_raft_dirs() {
    local out=""
    for id in $NODE_IDS; do out="$out $DATA_DIR/n$id/raft"; done
    echo "$out"
}

start_node() {
    local id=$1
    java -cp "$CP" kvstore.Main \
        --port "${CLIENT_PORT[$id]}" --data "$DATA_DIR/n$id/kvstore.wal" \
        --id "$id" --raft-port "${RAFT_PORT[$id]}" --peers "$(peers_for "$id")" \
        >>"${LOG[$id]}" 2>&1 &
    PID[$id]=$!
}

wait_ready() {
    local id=$1
    for _ in $(seq 1 100); do
        grep -q READY "${LOG[$id]}" 2>/dev/null && return 0
        kill -0 "${PID[$id]}" 2>/dev/null || { echo "  node $id died:"; sed 's/^/    /' "${LOG[$id]}"; return 1; }
        sleep 0.1
    done
    return 1
}

start_all() { for id in $NODE_IDS; do start_node "$id"; done; for id in $NODE_IDS; do wait_ready "$id" || return 1; done; }
kill_all()  { for id in $NODE_IDS; do kill -9 "${PID[$id]}" 2>/dev/null; done
              for id in $NODE_IDS; do wait "${PID[$id]}" 2>/dev/null; done; }

# Counts only elections announced since a marker line, so a re-election is not
# confused with the original one still sitting in an appended log file.
wait_for_leader_after() {
    local marker=$1
    for _ in $(seq 1 120); do
        for id in $NODE_IDS; do
            if awk "/$marker/{seen=1} seen && /elected leader/{found=1} END{exit !found}" "${LOG[$id]}" 2>/dev/null; then
                echo "$id"; return 0
            fi
        done
        sleep 0.1
    done
    return 1
}

mark_all() { for id in $NODE_IDS; do echo "=== $1 ===" >> "${LOG[$id]}"; done; }

echo "=============================================="
echo " Restart test: durable term, vote, and log"
echo " keys=$KEYS value=${VALUE_SIZE}B"
echo "=============================================="

rm -rf "$DATA_DIR"; mkdir -p "$DATA_DIR"

# ---------------------------------------------------------------- case 1
echo
echo "[1] Whole cluster dies and comes back"
mark_all RUN1
start_all || exit 1
LEADER=$(wait_for_leader_after RUN1) || { fail "initial election"; kill_all; exit 1; }
echo "  leader: node $LEADER"

java -cp "$CP" kvstore.tools.ClusterWorkload write "$KEYS" "$VALUE_SIZE" $(all_client_addrs) \
    >"$DATA_DIR/writer.log" 2>&1 \
    && echo "  wrote $KEYS keys" \
    || { echo "  writer failed:"; sed 's/^/    /' "$DATA_DIR/writer.log"; fail "initial write"; }

# SIGKILL every node at once: nothing gets a chance to flush on the way out, so
# anything that survives survived because it was already on disk.
kill_all
echo "  SIGKILLed all 3 nodes"

mark_all RUN2
start_all || exit 1
if LEADER=$(wait_for_leader_after RUN2); then
    pass "cluster elected a leader again after a full restart (node $LEADER)"
else
    fail "no leader after full restart"
fi

echo "  recovered state:"
# Per file, not across all three: each log holds every run's startup line, so a
# tail over the concatenation would mix one node's restart with another's.
for id in $NODE_IDS; do
    echo "    node $id: $(grep "raft recovered" "${LOG[$id]}" | tail -1 | sed 's/^raft recovered //')"
done

if java -cp "$CP" kvstore.tools.ClusterWorkload verify "$KEYS" "$VALUE_SIZE" $(all_client_addrs) | sed 's/^/  /'; then
    pass "all $KEYS keys survived the whole cluster being killed"
else
    fail "data lost across a full-cluster restart"
fi

# ---------------------------------------------------------------- case 2
echo
echo "[2] A single node restarts and rejoins"
FOLLOWER=$(for id in $NODE_IDS; do [ "$id" != "$LEADER" ] && echo "$id" && break; done)
kill -9 "${PID[$FOLLOWER]}" 2>/dev/null; wait "${PID[$FOLLOWER]}" 2>/dev/null
echo "  SIGKILLed follower node $FOLLOWER"

# The remaining two are still a majority, so writes must keep working.
if java -cp "$CP" kvstore.tools.ClusterWorkload write 25 "$VALUE_SIZE" $(all_client_addrs) >/dev/null 2>&1; then
    pass "cluster still accepted writes with one node down"
else
    fail "writes blocked while a minority was down"
fi

mark_all RUN3
start_node "$FOLLOWER"
wait_ready "$FOLLOWER" || fail "restarted node did not become ready"
sleep 1.5 # let the leader replicate everything it missed

if java -cp "$CP" kvstore.tools.ClusterWorkload verify "$KEYS" "$VALUE_SIZE" $(all_client_addrs) >/dev/null 2>&1; then
    pass "restarted node rejoined without breaking cluster consistency"
else
    fail "cluster inconsistent after a node rejoined"
fi

# ---------------------------------------------------------------- case 3
echo
echo "[3] The vote invariant, checked against what is actually on disk"
kill_all
if java -cp "$CP" kvstore.raft.RaftStateDump $(all_raft_dirs) | sed 's/^/  /'; then
    pass "no node ever recorded two different votes in one term"
else
    fail "vote invariant violated - two leaders could be elected for one term"
fi

echo
echo "=============================================="
echo " $PASS passed, $FAIL failed"
echo "=============================================="
[ "$FAIL" -eq 0 ]
