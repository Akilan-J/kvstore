package kvstore.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

import kvstore.net.ProtocolReader;
import kvstore.raft.RaftNode;
import kvstore.store.KeyValueStore;

/**
 * Serves one client connection until it closes or errors.
 *
 * <p>Protocol (requests):
 *
 * <pre>
 *   PING
 *   GET &lt;key&gt;
 *   DEL &lt;key&gt;
 *   PUT &lt;key&gt; &lt;nbytes&gt;\r\n&lt;nbytes of value&gt;\r\n
 *   STATS
 *   QUIT
 * </pre>
 *
 * <p>Replies: {@code +OK} / {@code +PONG} for simple strings, {@code $n} followed
 * by n bytes for a value ({@code $-1} for a miss), {@code :1} / {@code :0} for
 * integers, {@code -ERR msg} for errors. Every reply is CRLF-terminated.
 *
 * <p>The shape is deliberately close to Redis's RESP so the framing is familiar,
 * but it is not wire-compatible.
 *
 * <p>When Raft is enabled ({@code raftNode != null}), PUT and DEL are submitted
 * to the log and don't reply OK until a majority has committed them — this is
 * what "writes acknowledged after quorum" means in practice. GET is rejected
 * unless this node currently believes itself to be leader, which stops a
 * follower from ever answering with stale data.
 *
 * <p><b>Known gap:</b> "believes itself to be leader" is just this node's own
 * {@code role}, not a fresh confirmation that it can still reach a majority
 * right now. A leader cut off by a network partition can go on thinking it's
 * leader — and serving reads on that belief — for up to one election timeout
 * after the rest of the cluster has already elected someone else. Real Raft
 * closes this with a "read index" (confirm leadership via one heartbeat round
 * before answering) or leader leases; neither is implemented here.
 */
final class ConnectionHandler implements Runnable {

    private static final int MAX_VALUE_BYTES = 16 * 1024 * 1024;
    private static final int MAX_KEY_CHARS = 1024;

    // Comfortably above one election timeout's worth of retries, so a write
    // submitted just as the leader dies still has a chance to land on the node
    // that wins the next election, without leaving a client hanging forever.
    private static final long COMMIT_TIMEOUT_MILLIS = 2000;

    private final Socket socket;
    private final KeyValueStore store;
    private final RaftNode raftNode; // null in stage-1 single-node mode

    ConnectionHandler(Socket socket, KeyValueStore store, RaftNode raftNode) {
        this.socket = socket;
        this.store = store;
        this.raftNode = raftNode;
    }

    @Override
    public void run() {
        try (Socket s = socket;
                BufferedOutputStream out = new BufferedOutputStream(s.getOutputStream(), 1 << 16)) {
            ProtocolReader in = new ProtocolReader(new BufferedInputStream(s.getInputStream(), 1 << 16));
            s.setTcpNoDelay(true);
            serve(in, out);
        } catch (EOFException | SocketTimeoutException e) {
            // client vanished mid-frame or went idle; nothing to do
        } catch (IOException e) {
            // connection-level failure: log and drop this client only
            System.err.println("connection error: " + e.getMessage());
        }
    }

    private void serve(ProtocolReader in, OutputStream out) throws IOException {
        while (true) {
            String line = in.readLine();
            if (line == null) {
                return; // clean disconnect
            }
            if (line.isEmpty()) {
                continue;
            }

            String[] parts = line.split(" ");
            String command = parts[0].toUpperCase();

            switch (command) {
                case "PING" -> writeSimple(out, "PONG");
                case "GET" -> handleGet(parts, out);
                case "PUT" -> handlePut(parts, in, out);
                case "DEL" -> handleDelete(parts, out);
                case "STATS" -> writeSimple(out, store.stats());
                case "QUIT" -> {
                    writeSimple(out, "OK");
                    return;
                }
                default -> writeError(out, "unknown command '" + command + "'");
            }
        }
    }

    private void handleGet(String[] parts, OutputStream out) throws IOException {
        if (parts.length != 2) {
            writeError(out, "usage: GET <key>");
            return;
        }
        if (raftNode != null && !raftNode.canServeReads()) {
            if (raftNode.role() == RaftNode.Role.LEADER) {
                // Leader, but hasn't finished applying what it inherited. Answering
                // now could miss an already-acknowledged write, so refuse and let
                // the client retry a moment later.
                writeError(out, "leader is still catching up; retry");
            } else {
                writeError(out, "not leader (" + leaderHint() + ")");
            }
            return;
        }
        byte[] value = store.get(parts[1]);
        if (value == null) {
            out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
        } else {
            out.write(("$" + value.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(value);
            out.write('\r');
            out.write('\n');
        }
        out.flush();
    }

    private void handlePut(String[] parts, ProtocolReader in, OutputStream out) throws IOException {
        if (parts.length != 3) {
            writeError(out, "usage: PUT <key> <nbytes>");
            return;
        }
        String key = parts[1];
        if (key.isEmpty() || key.length() > MAX_KEY_CHARS) {
            writeError(out, "key must be 1.." + MAX_KEY_CHARS + " chars");
            return;
        }

        int length;
        try {
            length = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            writeError(out, "nbytes must be an integer");
            return;
        }
        if (length < 0 || length > MAX_VALUE_BYTES) {
            // Note: we reject before reading, so the connection is now out of sync
            // with the client's pending payload. Closing is the honest response.
            writeError(out, "nbytes must be 0.." + MAX_VALUE_BYTES + "; closing connection");
            throw new IOException("invalid payload length " + length);
        }

        byte[] value = in.readExactly(length);
        in.consumeTerminator();

        if (raftNode != null) {
            if (awaitCommit(out, () -> raftNode.submitPut(key, value, COMMIT_TIMEOUT_MILLIS))) {
                writeSimple(out, "OK");
            }
            return;
        }
        store.put(key, value);
        writeSimple(out, "OK");
    }

    private void handleDelete(String[] parts, OutputStream out) throws IOException {
        if (parts.length != 2) {
            writeError(out, "usage: DEL <key>");
            return;
        }
        String key = parts[1];

        if (raftNode != null) {
            if (awaitCommit(out, () -> raftNode.submitDelete(key, COMMIT_TIMEOUT_MILLIS))) {
                // Known simplification: a replicated delete always reports "removed"
                // once committed. Whether the key actually existed at that point in
                // the log isn't threaded back from the apply loop, unlike the direct
                // (non-Raft) path below, which reports it exactly.
                out.write(":1\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
            }
            return;
        }
        boolean removed = store.delete(key);
        out.write((removed ? ":1\r\n" : ":0\r\n").getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    /**
     * Submits a Raft write and waits for it to commit, writing an error reply and
     * returning false if it doesn't; the caller writes the success reply itself
     * since PUT and DEL report success differently.
     */
    private boolean awaitCommit(OutputStream out, RaftWrite write) throws IOException {
        boolean committed;
        try {
            committed = write.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writeError(out, "interrupted while waiting for commit");
            return false;
        }
        if (!committed) {
            writeError(out, "not leader, or write did not commit in time (" + leaderHint() + ")");
            return false;
        }
        return true;
    }

    private String leaderHint() {
        Integer leader = raftNode.leaderId();
        return (leader == null) ? "no known leader" : "leader is node " + leader;
    }

    @FunctionalInterface
    private interface RaftWrite {
        boolean run() throws InterruptedException;
    }

    private static void writeSimple(OutputStream out, String message) throws IOException {
        out.write(("+" + message + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static void writeError(OutputStream out, String message) throws IOException {
        out.write(("-ERR " + message + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
