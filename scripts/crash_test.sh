#!/usr/bin/env bash
# Durability test suite.
#
# Each case kills or corrupts the node and asserts what should and should not
# survive. Run from the project root: ./scripts/crash_test.sh
set -uo pipefail

CP=build
PORT=${PORT:-7399}
HOST=127.0.0.1
KEYS=${KEYS:-5000}
VALUE_SIZE=${VALUE_SIZE:-64}
DATA=/tmp/kvtest/kvstore.wal
LOG=/tmp/kvtest/server.log

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

start_server() {
    local sync=${1:-every}
    mkdir -p "$(dirname "$DATA")"
    java -cp "$CP" kvstore.Main --port "$PORT" --data "$DATA" --sync "$sync" >"$LOG" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 100); do
        if grep -q READY "$LOG" 2>/dev/null; then return 0; fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "  server died on startup:"; sed 's/^/    /' "$LOG"; return 1
        fi
        sleep 0.1
    done
    echo "  server did not become ready"; return 1
}

# SIGKILL: no shutdown hook, no flush, no chance to clean up. This is the whole
# point of the test - a graceful stop would prove nothing about the log.
kill_server_hard() {
    kill -9 "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
}

stop_server_clean() {
    kill -TERM "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
}

recovered_count() { grep -o 'recovered [0-9]*' "$LOG" | tail -1 | awk '{print $2}'; }

echo "=============================================="
echo " Durability test suite"
echo " keys=$KEYS value=${VALUE_SIZE}B port=$PORT"
echo "=============================================="

# ---------------------------------------------------------------- case 1
echo
echo "[1] SIGKILL after acknowledged writes"
rm -rf /tmp/kvtest
start_server every || exit 1
java -cp "$CP" kvstore.tools.Workload write "$HOST" "$PORT" "$KEYS" "$VALUE_SIZE" | sed 's/^/  /'
SIZE_BEFORE=$(wc -c < "$DATA" | tr -d ' ')
kill_server_hard
echo "  killed -9; log is $SIZE_BEFORE bytes"

start_server every || exit 1
echo "  $(grep recovered "$LOG" | tail -1)"
if java -cp "$CP" kvstore.tools.Workload verify "$HOST" "$PORT" "$KEYS" "$VALUE_SIZE" | sed 's/^/  /'; then
    pass "all $KEYS acknowledged writes survived SIGKILL"
else
    fail "data loss after SIGKILL"
fi
[ "$(recovered_count)" = "$KEYS" ] \
    && pass "replayed exactly $KEYS records" \
    || fail "replayed $(recovered_count), expected $KEYS"
stop_server_clean

# ---------------------------------------------------------------- case 2
echo
echo "[2] Deletes are durable too"
start_server every || exit 1
java -cp "$CP" -e 2>/dev/null || true
cat > /tmp/kvtest/Del.java <<'EOF'
import kvstore.net.KVClient;
public class Del {
    public static void main(String[] a) throws Exception {
        try (KVClient c = new KVClient(a[0], Integer.parseInt(a[1]), 10000)) {
            for (int i = 0; i < 100; i++) c.delete("key:" + i);
            System.out.println("deleted 100 keys");
        }
    }
}
EOF
javac -cp "$CP" -d /tmp/kvtest /tmp/kvtest/Del.java
java -cp "$CP:/tmp/kvtest" Del "$HOST" "$PORT" | sed 's/^/  /'
kill_server_hard
start_server every || exit 1
if java -cp "$CP" kvstore.tools.Workload verify-absent "$HOST" "$PORT" 100 "$VALUE_SIZE" | sed 's/^/  /'; then
    pass "deleted keys stayed deleted across a crash"
else
    fail "deleted keys resurrected after restart"
fi
stop_server_clean

# ---------------------------------------------------------------- case 3
echo
echo "[3] Garbage appended to the log tail"
GOOD_SIZE=$(wc -c < "$DATA" | tr -d ' ')
head -c 137 /dev/urandom >> "$DATA"
echo "  appended 137 random bytes ($GOOD_SIZE -> $(wc -c < "$DATA" | tr -d ' '))"
start_server every || exit 1
echo "  $(grep recovered "$LOG" | tail -1)"
AFTER_SIZE=$(wc -c < "$DATA" | tr -d ' ')
if [ "$AFTER_SIZE" = "$GOOD_SIZE" ]; then
    pass "corrupt tail truncated back to $GOOD_SIZE bytes"
else
    fail "log is $AFTER_SIZE bytes, expected truncation to $GOOD_SIZE"
fi
if java -cp "$CP" kvstore.tools.Workload verify "$HOST" "$PORT" 100 "$VALUE_SIZE" 2>/dev/null >/dev/null; then
    fail "keys 0-99 were deleted in case 2 and should still be absent"
else
    pass "surviving state is consistent with the pre-corruption log"
fi
stop_server_clean

# ---------------------------------------------------------------- case 4
echo
echo "[4] Log truncated mid-record (simulated torn write)"
TRUNC_TO=$(( $(wc -c < "$DATA" | tr -d ' ') - 40 ))
truncate -s "$TRUNC_TO" "$DATA"
echo "  truncated log to $TRUNC_TO bytes, mid-record"
start_server every || exit 1
if grep -q READY "$LOG"; then
    echo "  $(grep recovered "$LOG" | tail -1)"
    pass "node started cleanly from a torn log instead of crashing"
else
    fail "node failed to start from a torn log"
fi
NEW_SIZE=$(wc -c < "$DATA" | tr -d ' ')
[ "$NEW_SIZE" -le "$TRUNC_TO" ] \
    && pass "torn record dropped (log now $NEW_SIZE bytes)" \
    || fail "log grew to $NEW_SIZE without a write"
# The store must still accept traffic after recovering from damage.
java -cp "$CP" kvstore.tools.Workload write "$HOST" "$PORT" 50 "$VALUE_SIZE" >/dev/null 2>&1 \
    && java -cp "$CP" kvstore.tools.Workload verify "$HOST" "$PORT" 50 "$VALUE_SIZE" >/dev/null 2>&1 \
    && pass "writes still work after recovering from a torn log" \
    || fail "store unusable after torn-log recovery"
stop_server_clean

echo
echo "=============================================="
echo " $PASS passed, $FAIL failed"
echo "=============================================="
[ "$FAIL" -eq 0 ]
