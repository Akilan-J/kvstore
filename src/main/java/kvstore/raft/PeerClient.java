package kvstore.raft;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import kvstore.net.ProtocolReader;

/**
 * Outbound connection to one peer's Raft port.
 *
 * <p>Unlike {@code kvstore.net.KVClient}, this reconnects lazily instead of
 * staying dead after one failure: peers restart independently, and once
 * heartbeats exist a leader must keep retrying a down follower every ~100ms
 * rather than giving up the first time a connection breaks. Not thread-safe —
 * callers must serialize their own access (the election/heartbeat stage adds
 * one owning thread per peer, so this won't need a lock).
 */
public final class PeerClient implements Closeable {

    private final InetSocketAddress address;
    private final int timeoutMillis;

    private Socket socket;
    private OutputStream out;
    private ProtocolReader in;

    public PeerClient(InetSocketAddress address, int timeoutMillis) {
        this.address = address;
        this.timeoutMillis = timeoutMillis;
    }

    /** Asks this peer for its vote in {@code term}. */
    public RaftRpc.VoteReply requestVote(long term, int candidateId) throws IOException {
        ensureConnected();
        try {
            out.write(("REQUEST_VOTE " + term + " " + candidateId + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            String reply = in.readLine();
            if (reply == null) {
                throw new IOException("peer closed connection during REQUEST_VOTE");
            }
            String[] parts = reply.split(" ");
            if (parts.length != 3 || !parts[0].equals("VOTE")) {
                throw new IOException("unexpected REQUEST_VOTE reply: " + reply);
            }
            return new RaftRpc.VoteReply(Long.parseLong(parts[1]), "1".equals(parts[2]));
        } catch (IOException | NumberFormatException e) {
            closeQuietly(); // drop the bad socket so the next call reconnects from scratch
            throw (e instanceof IOException io) ? io : new IOException(e);
        }
    }

    /** Sends a heartbeat (log-entry-free AppendEntries) and returns the peer's current term. */
    public long appendEntries(long term, int leaderId) throws IOException {
        ensureConnected();
        try {
            out.write(("APPEND_ENTRIES " + term + " " + leaderId + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            String reply = in.readLine();
            if (reply == null) {
                throw new IOException("peer closed connection during APPEND_ENTRIES");
            }
            String[] parts = reply.split(" ");
            if (parts.length != 2 || !parts[0].equals("APPEND_REPLY")) {
                throw new IOException("unexpected APPEND_ENTRIES reply: " + reply);
            }
            return Long.parseLong(parts[1]);
        } catch (IOException | NumberFormatException e) {
            closeQuietly();
            throw (e instanceof IOException io) ? io : new IOException(e);
        }
    }

    private void ensureConnected() throws IOException {
        if (socket != null && !socket.isClosed()) {
            return;
        }
        socket = new Socket();
        socket.connect(address, timeoutMillis);
        socket.setSoTimeout(timeoutMillis);
        socket.setTcpNoDelay(true);
        out = new BufferedOutputStream(socket.getOutputStream(), 1 << 16);
        in = new ProtocolReader(new BufferedInputStream(socket.getInputStream(), 1 << 16));
    }

    private void closeQuietly() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // best-effort; we're discarding this socket either way
        }
        socket = null;
    }

    @Override
    public void close() {
        closeQuietly();
    }
}
