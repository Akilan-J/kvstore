package kvstore.raft;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

    /**
     * Asks this peer for its vote in {@code term}. {@code lastLogIndex}/{@code lastLogTerm}
     * describe our own log so the peer can refuse a candidate whose log is behind
     * (Raft's election restriction — see {@link RaftNode#handleRequestVote}).
     */
    public RaftRpc.VoteReply requestVote(long term, int candidateId, int lastLogIndex, long lastLogTerm)
            throws IOException {
        ensureConnected();
        try {
            out.write(("REQUEST_VOTE " + term + " " + candidateId + " " + lastLogIndex + " " + lastLogTerm + "\r\n")
                    .getBytes(StandardCharsets.UTF_8));
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

    /**
     * Replicates {@code entries} (empty for a pure heartbeat) starting right after
     * {@code prevLogIndex}/{@code prevLogTerm}, and tells the follower everything up
     * to {@code leaderCommit} is safe to apply.
     */
    public RaftRpc.AppendReply appendEntries(long term, int leaderId, int prevLogIndex, long prevLogTerm,
            List<RaftLog.Entry> entries, int leaderCommit) throws IOException {
        ensureConnected();
        try {
            out.write((
                    "APPEND_ENTRIES " + term + " " + leaderId + " " + prevLogIndex + " " + prevLogTerm
                            + " " + leaderCommit + " " + entries.size() + "\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            for (RaftLog.Entry entry : entries) {
                byte[] command = entry.command();
                out.write((entry.term() + " " + command.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                out.write(command);
                out.write('\r');
                out.write('\n');
            }
            out.flush();

            String reply = in.readLine();
            if (reply == null) {
                throw new IOException("peer closed connection during APPEND_ENTRIES");
            }
            String[] parts = reply.split(" ");
            if (parts.length != 3 || !parts[0].equals("APPEND_REPLY")) {
                throw new IOException("unexpected APPEND_ENTRIES reply: " + reply);
            }
            return new RaftRpc.AppendReply(Long.parseLong(parts[1]), "1".equals(parts[2]));
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
