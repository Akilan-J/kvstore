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

    /** Sends PING and returns the echoed term, or throws if the peer is unreachable. */
    public long ping(long term) throws IOException {
        ensureConnected();
        try {
            out.write(("PING " + term + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            String reply = in.readLine();
            if (reply == null) {
                throw new IOException("peer closed connection during PING");
            }
            if (!reply.startsWith("PONG ")) {
                throw new IOException("unexpected PING reply: " + reply);
            }
            return Long.parseLong(reply.substring(5));
        } catch (IOException | NumberFormatException e) {
            closeQuietly(); // drop the bad socket so the next call reconnects from scratch
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
