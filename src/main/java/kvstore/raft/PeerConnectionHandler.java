package kvstore.raft;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import kvstore.net.ProtocolReader;

/**
 * Serves one inbound peer connection until it closes or errors.
 *
 * <p>Protocol so far — grows in later commits as more of Raft lands, the same
 * way {@code kvstore.server.ConnectionHandler}'s switch grew one client
 * command at a time:
 *
 * <pre>
 *   REQUEST_VOTE &lt;term&gt; &lt;candidateId&gt; &lt;lastLogIndex&gt; &lt;lastLogTerm&gt;
 *     -&gt; VOTE &lt;term&gt; &lt;granted:0|1&gt;
 *
 *   APPEND_ENTRIES &lt;term&gt; &lt;leaderId&gt; &lt;prevLogIndex&gt; &lt;prevLogTerm&gt; &lt;leaderCommit&gt; &lt;numEntries&gt;
 *   [&lt;entryTerm&gt; &lt;commandLen&gt;\r\n&lt;commandLen bytes&gt;\r\n]*numEntries
 *     -&gt; APPEND_REPLY &lt;term&gt; &lt;success:0|1&gt;
 * </pre>
 *
 * <p>{@code numEntries} is 0 for a pure heartbeat — the leader sends those on
 * a fixed interval regardless of whether there's anything new to replicate,
 * purely to stop followers from starting an election.
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
                case "APPEND_ENTRIES" -> handleAppendEntries(parts, in, out);
                default -> writeLine(out, "ERR unknown rpc '" + parts[0] + "'");
            }
        }
    }

    private void handleRequestVote(String[] parts, OutputStream out) throws IOException {
        if (parts.length != 5) {
            writeLine(out, "ERR usage: REQUEST_VOTE <term> <candidateId> <lastLogIndex> <lastLogTerm>");
            return;
        }
        long term;
        int candidateId;
        int lastLogIndex;
        long lastLogTerm;
        try {
            term = Long.parseLong(parts[1]);
            candidateId = Integer.parseInt(parts[2]);
            lastLogIndex = Integer.parseInt(parts[3]);
            lastLogTerm = Long.parseLong(parts[4]);
        } catch (NumberFormatException e) {
            writeLine(out, "ERR malformed REQUEST_VOTE header");
            return;
        }
        RaftRpc.VoteReply reply = raftNode.handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm);
        writeLine(out, "VOTE " + reply.term() + " " + (reply.granted() ? "1" : "0"));
    }

    private void handleAppendEntries(String[] parts, ProtocolReader in, OutputStream out) throws IOException {
        if (parts.length != 7) {
            writeLine(out, "ERR usage: APPEND_ENTRIES <term> <leaderId> <prevLogIndex> <prevLogTerm>"
                    + " <leaderCommit> <numEntries>");
            return;
        }
        long term;
        int leaderId;
        int prevLogIndex;
        long prevLogTerm;
        int leaderCommit;
        int numEntries;
        try {
            term = Long.parseLong(parts[1]);
            leaderId = Integer.parseInt(parts[2]);
            prevLogIndex = Integer.parseInt(parts[3]);
            prevLogTerm = Long.parseLong(parts[4]);
            leaderCommit = Integer.parseInt(parts[5]);
            numEntries = Integer.parseInt(parts[6]);
        } catch (NumberFormatException e) {
            writeLine(out, "ERR malformed APPEND_ENTRIES header");
            return;
        }

        List<RaftLog.Entry> entries = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String entryHeader = in.readLine();
            if (entryHeader == null) {
                throw new EOFException("connection closed mid-entry");
            }
            String[] entryParts = entryHeader.split(" ");
            if (entryParts.length != 2) {
                writeLine(out, "ERR malformed entry header");
                return;
            }
            long entryTerm = Long.parseLong(entryParts[0]);
            int commandLen = Integer.parseInt(entryParts[1]);
            byte[] command = in.readExactly(commandLen);
            in.consumeTerminator();
            entries.add(new RaftLog.Entry(entryTerm, command));
        }

        RaftRpc.AppendReply reply = raftNode.handleAppendEntries(term, leaderId, prevLogIndex, prevLogTerm,
                entries, leaderCommit);
        writeLine(out, "APPEND_REPLY " + reply.term() + " " + (reply.success() ? "1" : "0"));
    }

    private static void writeLine(OutputStream out, String line) throws IOException {
        out.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
