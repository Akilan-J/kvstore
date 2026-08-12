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
 * <p>Protocol so far — grows in later commits as log replication lands, the
 * same way {@code kvstore.server.ConnectionHandler}'s switch grew one client
 * command at a time:
 *
 * <pre>
 *   REQUEST_VOTE &lt;term&gt; &lt;candidateId&gt;   -&gt;   VOTE &lt;term&gt; &lt;granted:0|1&gt;
 *   APPEND_ENTRIES &lt;term&gt; &lt;leaderId&gt;    -&gt;   APPEND_REPLY &lt;term&gt;
 * </pre>
 *
 * <p>{@code APPEND_ENTRIES} carries no log entries yet — until log
 * replication lands it is purely the leader's heartbeat, sent on a fixed
 * interval to stop followers from starting an election.
 */
final class PeerConnectionHandler implements Runnable {

    private final Socket socket;
    private final RaftNode raftNode;

    PeerConnectionHandler(Socket socket, RaftNode raftNode) {
        this.socket = socket;
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
                case "REQUEST_VOTE" -> handleRequestVote(parts, out);
                case "APPEND_ENTRIES" -> handleAppendEntries(parts, out);
                default -> writeLine(out, "ERR unknown rpc '" + parts[0] + "'");
            }
        }
    }

    private void handleRequestVote(String[] parts, OutputStream out) throws IOException {
        if (parts.length != 3) {
            writeLine(out, "ERR usage: REQUEST_VOTE <term> <candidateId>");
            return;
        }
        long term;
        int candidateId;
        try {
            term = Long.parseLong(parts[1]);
            candidateId = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            writeLine(out, "ERR term and candidateId must be integers");
            return;
        }
        RaftRpc.VoteReply reply = raftNode.handleRequestVote(term, candidateId);
        writeLine(out, "VOTE " + reply.term() + " " + (reply.granted() ? "1" : "0"));
    }

    private void handleAppendEntries(String[] parts, OutputStream out) throws IOException {
        if (parts.length != 3) {
            writeLine(out, "ERR usage: APPEND_ENTRIES <term> <leaderId>");
            return;
        }
        long term;
        int leaderId;
        try {
            term = Long.parseLong(parts[1]);
            leaderId = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            writeLine(out, "ERR term and leaderId must be integers");
            return;
        }
        long replyTerm = raftNode.handleAppendEntries(term, leaderId);
        writeLine(out, "APPEND_REPLY " + replyTerm);
    }

    private static void writeLine(OutputStream out, String line) throws IOException {
        out.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
