package kvstore.raft;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

/**
 * Raft leader election: terms, votes, and randomised election timeouts.
 *
 * <p>No log yet — {@code AppendEntries} carries no entries, so this only
 * decides who is leader, not what gets replicated. That's the next stage.
 *
 * <p><b>Concurrency.</b> {@code currentTerm}, {@code votedFor}, {@code role},
 * and {@code leaderId} are all touched from several threads: the election
 * timer, one thread per peer, and whichever worker thread is handling an
 * inbound RPC from a peer. Every read or write of that group goes through
 * {@code lock} — the same reasoning as {@code KeyValueStore} serializing
 * writes on one lock, just applied to term/vote state instead of the map, so
 * a node can never do something like grant two votes in one term because two
 * RequestVote RPCs arrived on different connections at once.
 *
 * <p><b>One thread per peer.</b> Each peer gets a single long-lived thread
 * that owns that peer's {@link PeerClient} for the node's entire lifetime,
 * sending whatever RPC the current role calls for (heartbeats if we're
 * leader, a vote request if we're a candidate). This is what makes
 * PeerClient's "not thread-safe, one owner" contract hold: nothing else ever
 * touches that socket.
 */
public final class RaftNode {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    // Raft paper's own numbers: heartbeats well inside the minimum election
    // timeout so a healthy leader is never mistaken for a dead one.
    private static final long HEARTBEAT_INTERVAL_MILLIS = 50;
    private static final long ELECTION_TIMEOUT_MIN_MILLIS = 150;
    private static final long ELECTION_TIMEOUT_MAX_MILLIS = 300;
    private static final long PEER_LOOP_TICK_MILLIS = 20;

    private final int selfId;
    private final ClusterConfig cluster;
    private final Map<Integer, PeerClient> peerClients;
    private final Random random = new Random();

    private final Object lock = new Object();
    private Role role = Role.FOLLOWER;
    private long currentTerm = 0;
    private Integer votedFor = null;
    private Integer leaderId = null;
    private int votesGranted = 0;
    private long votesGrantedForTerm = -1;

    private volatile long electionDeadlineNanos;
    private volatile boolean running;

    public RaftNode(int selfId, ClusterConfig cluster, Map<Integer, PeerClient> peerClients) {
        this.selfId = selfId;
        this.cluster = cluster;
        this.peerClients = peerClients;
    }

    public void start() {
        running = true;
        resetElectionDeadline();

        Thread timer = new Thread(this::electionTimerLoop, "raft-timer");
        timer.setDaemon(true);
        timer.start();

        for (Map.Entry<Integer, PeerClient> entry : peerClients.entrySet()) {
            int peerId = entry.getKey();
            PeerClient client = entry.getValue();
            Thread peerLoop = new Thread(() -> runPeerLoop(peerId, client), "raft-peer-" + peerId);
            peerLoop.setDaemon(true);
            peerLoop.start();
        }
    }

    public void stop() {
        running = false;
    }

    public Role role() {
        synchronized (lock) {
            return role;
        }
    }

    public long currentTerm() {
        synchronized (lock) {
            return currentTerm;
        }
    }

    public Integer leaderId() {
        synchronized (lock) {
            return leaderId;
        }
    }

    // ---- election timeout ----------------------------------------------

    private void resetElectionDeadline() {
        long spanMillis = ELECTION_TIMEOUT_MAX_MILLIS - ELECTION_TIMEOUT_MIN_MILLIS;
        long timeoutMillis = ELECTION_TIMEOUT_MIN_MILLIS + random.nextInt((int) spanMillis);
        electionDeadlineNanos = System.nanoTime() + timeoutMillis * 1_000_000L;
    }

    private void electionTimerLoop() {
        while (running) {
            sleep(PEER_LOOP_TICK_MILLIS);
            boolean expired;
            synchronized (lock) {
                expired = role != Role.LEADER && System.nanoTime() >= electionDeadlineNanos;
            }
            if (expired) {
                startElection();
            }
        }
    }

    private void startElection() {
        long electionTerm;
        synchronized (lock) {
            currentTerm++;
            role = Role.CANDIDATE;
            votedFor = selfId;
            leaderId = null;
            votesGranted = 1; // vote for self
            votesGrantedForTerm = currentTerm;
            electionTerm = currentTerm;
            resetElectionDeadline(); // covers a split vote: this candidate must be able to retry
            System.out.printf("[node %d] election timeout, starting election for term %d%n", selfId, electionTerm);
        }
        // Nothing else to do here: each peer loop notices the new term/role on its
        // next tick and sends its own RequestVote.
    }

    // ---- outbound: one thread per peer, for the node's whole lifetime ---

    private void runPeerLoop(int peerId, PeerClient client) {
        long lastVoteRequestedTerm = -1;
        while (running) {
            Role currentRole;
            long term;
            synchronized (lock) {
                currentRole = role;
                term = currentTerm;
            }
            switch (currentRole) {
                case LEADER -> {
                    try {
                        long replyTerm = client.appendEntries(term, selfId);
                        stepDownIfNewerTerm(replyTerm);
                    } catch (IOException e) {
                        // peer down or unreachable; next heartbeat tick retries
                    }
                    sleep(HEARTBEAT_INTERVAL_MILLIS);
                }
                case CANDIDATE -> {
                    if (term != lastVoteRequestedTerm) {
                        lastVoteRequestedTerm = term;
                        try {
                            RaftRpc.VoteReply reply = client.requestVote(term, selfId);
                            if (reply.term() > term) {
                                stepDownIfNewerTerm(reply.term());
                            } else if (reply.granted()) {
                                recordVoteGranted(term, peerId);
                            }
                        } catch (IOException e) {
                            // peer unreachable for this round; a new election (new term) will retry
                        }
                    }
                    sleep(PEER_LOOP_TICK_MILLIS);
                }
                case FOLLOWER -> sleep(PEER_LOOP_TICK_MILLIS);
            }
        }
        client.close();
    }

    private void recordVoteGranted(long term, int peerId) {
        synchronized (lock) {
            if (role != Role.CANDIDATE || currentTerm != term || votesGrantedForTerm != term) {
                return; // stale reply: we moved on to a new term or role since asking
            }
            votesGranted++;
            System.out.printf("[node %d] got vote from %d for term %d (%d/%d)%n",
                    selfId, peerId, term, votesGranted, cluster.clusterSize());
            if (votesGranted >= cluster.quorumSize()) {
                role = Role.LEADER;
                leaderId = selfId;
                System.out.printf("[node %d] elected leader for term %d%n", selfId, term);
            }
        }
    }

    private void stepDownIfNewerTerm(long newTerm) {
        synchronized (lock) {
            if (newTerm > currentTerm) {
                currentTerm = newTerm;
                votedFor = null;
                role = Role.FOLLOWER;
                leaderId = null;
                resetElectionDeadline();
            }
        }
    }

    // ---- inbound: called from PeerConnectionHandler worker threads ------

    /** Raft paper's RequestVote receiver rules, minus the log up-to-date check (no log yet). */
    RaftRpc.VoteReply handleRequestVote(long term, int candidateId) {
        synchronized (lock) {
            if (term < currentTerm) {
                return new RaftRpc.VoteReply(currentTerm, false);
            }
            if (term > currentTerm) {
                currentTerm = term;
                votedFor = null;
                role = Role.FOLLOWER;
            }
            boolean granted = votedFor == null || votedFor == candidateId;
            if (granted) {
                votedFor = candidateId;
                // Granting a vote means we just heard from a legitimate participant in
                // this term's election, so give it the full timeout before we compete.
                resetElectionDeadline();
            }
            return new RaftRpc.VoteReply(currentTerm, granted);
        }
    }

    long handleAppendEntries(long term, int leaderIdFromRpc) {
        synchronized (lock) {
            if (term < currentTerm) {
                return currentTerm; // stale leader; it will step down once it sees our term
            }
            if (term > currentTerm) {
                currentTerm = term;
                votedFor = null;
            }
            role = Role.FOLLOWER; // a current-or-newer-term leader outranks a lingering candidate
            leaderId = leaderIdFromRpc;
            resetElectionDeadline();
            return currentTerm;
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
