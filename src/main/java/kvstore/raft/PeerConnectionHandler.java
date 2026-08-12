package kvstore.raft;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

import kvstore.net.ProtocolReader;

/**
 * Serves one inbound peer connection until it closes or errors.
 *
 * <p>Protocol so far — grows in later commits as election and log replication
 * land, the same way {@code kvstore.server.ConnectionHandler}'s switch grew
 * one client command at a time:
 *
 * <pre>
 *   PING &lt;term&gt;   -&gt;   PONG &lt;term&gt;
 * </pre>
 *
 * <p>{@code PING} carries a term only to prove the framing can carry Raft
 * state end to end; it does not yet affect any term/vote bookkeeping — there
 * is none yet. That starts with leader election.
 */
final class PeerConnectionHandler implements Runnable {

    private final Socket socket;

    PeerConnectionHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try (Socket s = socket;
                BufferedOutputStream out = new BufferedOutputStream(s.getOutputStream(), 1 << 16)) {
            ProtocolReader in = new ProtocolReader(new BufferedInputStream(s.getInputStream(), 1 << 16));
            s.setTcpNoDelay(true);
            serve(in, out);
        } catch (EOFException | SocketTimeoutException e) {
            // peer vanished mid-frame, or the connection sat idle past the timeout
        } catch (IOException e) {
            System.err.println("peer connection error: " + e.getMessage());
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
            switch (parts[0]) {
                case "PING" -> handlePing(parts, out);
                default -> writeLine(out, "ERR unknown rpc '" + parts[0] + "'");
            }
        }
    }

    private void handlePing(String[] parts, OutputStream out) throws IOException {
        if (parts.length != 2) {
            writeLine(out, "ERR usage: PING <term>");
            return;
        }
        writeLine(out, "PONG " + parts[1]);
    }

    private static void writeLine(OutputStream out, String line) throws IOException {
        out.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
