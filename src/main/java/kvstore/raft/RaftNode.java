package kvstore.raft;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import kvstore.store.KeyValueStore;
import kvstore.wal.WriteAheadLog;

/**
 * Raft leader election plus log replication: terms, votes, randomised
 * election timeouts, and a replicated log with a commit index.
 *
 * <p><b>Persistence.</b> {@code currentTerm}, {@code votedFor}, and the log are
 * written to {@link RaftStorage} before this node replies to any RPC that
 * depended on them — the paper's rule, and the reason a restarted node can't
 * vote twice in one term and hand two leaders the same term. Every write path
 * below that touches those three calls {@code persistState()} or goes through
 * {@link RaftLog}, which writes through to disk itself.
 *
 * <p>The cost is an fsync while holding {@code lock}, which stalls heartbeats
 * and votes for its duration. That is the honest price of the guarantee: term
 * and vote changes happen on elections rather than on writes, so it is paid
 * rarely. {@code lastApplied} is the exception — it moves with every applied
 * entry, so it is checkpointed every {@link #APPLY_CHECKPOINT_INTERVAL}
 * applies instead of every one.
 *
 * <p>{@code commitIndex} and {@code role} are deliberately <em>not</em>
 * persisted; the paper treats both as volatile. A restarted node comes back a
 * follower with {@code commitIndex} 0 and relearns what is committed from the
 * leader's next {@code AppendEntries}.
 *
 * <p><b>Concurrency.</b> {@code currentTerm}, {@code votedFor}, {@code role},
 * {@code leaderId}, the log, and the commit/apply indexes are all touched
 * from several threads: the election timer, one thread per peer, the apply
 * loop, and whichever worker thread is handling an inbound RPC. Every read or
 * write of that group goes through {@code lock} — the same reasoning as
 * {@code KeyValueStore} serializing writes on one lock, just applied to Raft
 * state instead of the map.
 *
 * <p><b>One thread per peer.</b> Each peer gets a single long-lived thread
 * that owns that peer's {@link PeerClient} for the node's entire lifetime,
 * sending whatever RPC the current role calls for (heartbeats/replication if
 * we're leader, a vote request if we're a candidate). This is what makes
 * PeerClient's "not thread-safe, one owner" contract hold: nothing else ever
 * touches that socket.
 *
 * <p><b>Applying committed entries</b> happens on its own thread rather than
 * inline in the peer loops or the RPC handlers: applying means calling into
 * {@code KeyValueStore}, which does its own disk I/O, and that shouldn't
 * block replication traffic or vote handling.
 */
public final class RaftNode {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    // Raft paper's own numbers: heartbeats well inside the minimum election
    // timeout so a healthy leader is never mistaken for a dead one.
    private static final long HEARTBEAT_INTERVAL_MILLIS = 50;
    private static final long ELECTION_TIMEOUT_MIN_MILLIS = 150;
    private static final long ELECTION_TIMEOUT_MAX_MILLIS = 300;
    private static final long PEER_LOOP_TICK_MILLIS = 20;

    /**
     * How many applies pass between {@code lastApplied} checkpoints. Persisting
     * on every apply would add an fsync to every committed write; persisting
     * rarely is safe because applies are idempotent and always replayed in log
     * order, so a checkpoint that lags just means a bounded amount of harmless
     * re-application after a restart.
     */
    private static final int APPLY_CHECKPOINT_INTERVAL = 64;

    private final int selfId;
    private final ClusterConfig cluster;
    private final Map<Integer, PeerClient> peerClients;
    private final KeyValueStore store;
    private final RaftStorage storage;
    private final Random random = new Random();

    private final Object lock = new Object();
    private Role role = Role.FOLLOWER;
    private long currentTerm = 0;
    private Integer votedFor = null;
    private Integer leaderId = null;
    private int votesGranted = 0;
    private long votesGrantedForTerm = -1;

    private final RaftLog log;
    private int commitIndex = 0;
    private int lastApplied = 0;
    private int lastCheckpointedApplied = 0;
    // Leader-only; rebuilt from scratch each time this node wins an election.
    private Map<Integer, Integer> nextIndex;
    private Map<Integer, Integer> matchIndex;

    private volatile long electionDeadlineNanos;
    private volatile boolean running;

    /**
     * @param raftDir directory holding this node's {@code raft.state} and
     *     {@code raft.log}; must not be shared with another node
     * @param syncPolicy whether each durable write is fsync'd, mirroring the
     *     store's own {@code --sync} flag
     */
    public RaftNode(int selfId, ClusterConfig cluster, Map<Integer, PeerClient> peerClients, KeyValueStore store,
            Path raftDir, WriteAheadLog.SyncPolicy syncPolicy) throws IOException {
        this.selfId = selfId;
        this.cluster = cluster;
        this.peerClients = peerClients;
        this.store = store;
        this.storage = new RaftStorage(raftDir, syncPolicy);
        this.log = new RaftLog(storage);

        RaftStorage.PersistentState recovered = storage.loadState();
        this.currentTerm = recovered.currentTerm();
        this.votedFor = recovered.votedFor();
        // Resume applying from where the last checkpoint says we got to. It may
        // lag behind what was actually applied, which only costs us re-applying
        // a few entries; it can never run ahead, because the checkpoint is
        // written after the applies it covers.
        this.lastApplied = recovered.lastApplied();
        this.lastCheckpointedApplied = this.lastApplied;
        this.log.restore(storage.loadLog());
    }

    /** What recovery found on disk, for logging at startup. */
    public String recoveredSummary() {
        synchronized (lock) {
            return String.format("term=%d votedFor=%s log_entries=%d last_applied=%d",
                    currentTerm, votedFor == null ? "none" : votedFor, log.lastIndex(), lastApplied);
        }
    }

    public void start() {
        running = true;
        resetElectionDeadline();

        Thread timer = new Thread(this::electionTimerLoop, "raft-timer");
        timer.setDaemon(true);
        timer.start();

        Thread applier = new Thread(this::applyLoop, "raft-apply");
        applier.setDaemon(true);
        applier.start();

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
        // Checkpoint apply progress on the way out so a graceful restart doesn't
        // redo up to APPLY_CHECKPOINT_INTERVAL entries for no reason. Best-effort:
        // a SIGKILL skips this entirely, which is exactly the case the
        // apply-then-checkpoint ordering already makes safe.
        synchronized (lock) {
            try {
                storage.saveState(currentTerm, votedFor, lastApplied);
                lastCheckpointedApplied = lastApplied;
            } catch (IOException e) {
                System.err.printf("[node %d] final state checkpoint failed: %s%n", selfId, e.getMessage());
            }
        }
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

    /** Leader-only: replicates a PUT and blocks until it commits or the timeout elapses. */
    public boolean submitPut(String key, byte[] value, long timeoutMillis) throws InterruptedException {
        return submitAndAwaitCommit(Command.put(key, value), timeoutMillis);
    }

    /** Leader-only: replicates a DELETE and blocks until it commits or the timeout elapses. */
    public boolean submitDelete(String key, long timeoutMillis) throws InterruptedException {
        return submitAndAwaitCommit(Command.delete(key), timeoutMillis);
    }

    /**
     * Appends {@code command} to the log and blocks until it commits (replicated
     * to a majority) or {@code timeoutMillis} elapses.
     *
     * @return true once committed; false if this node was never leader for it,
     *     lost leadership before it committed, or timed out waiting
     */
    private boolean submitAndAwaitCommit(Command command, long timeoutMillis) throws InterruptedException {
        int index;
        long term;
        synchronized (lock) {
            if (role != Role.LEADER) {
                return false;
            }
            term = currentTerm;
            try {
                // Durable on the leader before it is replicated anywhere, so a leader
                // that crashes and returns still has every entry it ever offered.
                index = log.append(term, command.encode());
            } catch (IOException e) {
                throw new PersistenceFailure("could not persist submitted entry on node " + selfId, e);
            }
        }

        long deadlineNanos = System.nanoTime() + timeoutMillis * 1_000_000L;
        synchronized (lock) {
            while (commitIndex < index) {
                if (role != Role.LEADER || currentTerm != term) {
                    return false; // stepped down, or superseded, before this committed
                }
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return false;
                }
                long remainingMillis = remainingNanos / 1_000_000L;
                lock.wait(remainingMillis, (int) (remainingNanos - remainingMillis * 1_000_000L));
            }
            return true;
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
            // Durable before we ask anyone for a vote: if we crash here and come
            // back, we must remember we already spent our own vote on this term.
            persistState();
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
                    replicateTo(peerId, client, term);
                    sleep(HEARTBEAT_INTERVAL_MILLIS);
                }
                case CANDIDATE -> {
                    if (term != lastVoteRequestedTerm) {
                        lastVoteRequestedTerm = term;
                        requestVoteFrom(peerId, client, term);
                    }
                    sleep(PEER_LOOP_TICK_MILLIS);
                }
                case FOLLOWER -> sleep(PEER_LOOP_TICK_MILLIS);
            }
        }
        client.close();
    }

    private void requestVoteFrom(int peerId, PeerClient client, long term) {
        int lastLogIndex;
        long lastLogTerm;
        synchronized (lock) {
            lastLogIndex = log.lastIndex();
            lastLogTerm = log.termAt(lastLogIndex);
        }
        try {
            RaftRpc.VoteReply reply = client.requestVote(term, selfId, lastLogIndex, lastLogTerm);
            if (reply.term() > term) {
                stepDownIfNewerTerm(reply.term());
            } else if (reply.granted()) {
                recordVoteGranted(term, peerId);
            }
        } catch (IOException e) {
            // peer unreachable for this round; a new election (new term) will retry
        }
    }

    private void replicateTo(int peerId, PeerClient client, long term) {
        int peerNextIndex;
        int prevLogIndex;
        long prevLogTerm;
        List<RaftLog.Entry> entries;
        int leaderCommitSnapshot;
        synchronized (lock) {
            peerNextIndex = nextIndex.get(peerId);
            prevLogIndex = peerNextIndex - 1;
            prevLogTerm = log.termAt(prevLogIndex);
            entries = log.entriesFrom(peerNextIndex);
            leaderCommitSnapshot = commitIndex;
        }
        try {
            RaftRpc.AppendReply reply =
                    client.appendEntries(term, selfId, prevLogIndex, prevLogTerm, entries, leaderCommitSnapshot);
            stepDownIfNewerTerm(reply.term());
            synchronized (lock) {
                if (role != Role.LEADER || currentTerm != term) {
                    return; // no longer leader for the term this round was sent under
                }
                if (reply.success()) {
                    int newMatchIndex = prevLogIndex + entries.size();
                    matchIndex.put(peerId, newMatchIndex);
                    nextIndex.put(peerId, newMatchIndex + 1);
                    tryAdvanceCommitIndex();
                } else {
                    // Simple backtrack, one index at a time: not the paper's optional fast
                    // backtrack-by-term optimisation, but a demo-scale log catches up in a
                    // handful of heartbeats either way, and this is far easier to explain.
                    nextIndex.put(peerId, Math.max(1, peerNextIndex - 1));
                }
            }
        } catch (IOException e) {
            // peer down or unreachable; next heartbeat tick retries
        }
    }

    /** Must be called with {@code lock} held. Raft §5.4.2: only commit entries from our own term directly. */
    private void tryAdvanceCommitIndex() {
        for (int n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != currentTerm) {
                continue;
            }
            int replicatedCount = 1; // this node already has it
            for (int match : matchIndex.values()) {
                if (match >= n) {
                    replicatedCount++;
                }
            }
            if (replicatedCount >= cluster.quorumSize()) {
                commitIndex = n;
                lock.notifyAll(); // wakes submitAndAwaitCommit waiters and the apply loop
                return;
            }
        }
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
                becomeLeader(term);
            }
        }
    }

    /** Must be called with {@code lock} held. */
    private void becomeLeader(long term) {
        role = Role.LEADER;
        leaderId = selfId;

        nextIndex = new HashMap<>();
        matchIndex = new HashMap<>();
        for (int peerId : peerClients.keySet()) {
            nextIndex.put(peerId, log.lastIndex() + 1);
            matchIndex.put(peerId, 0);
        }
        lock.notifyAll();
        System.out.printf("[node %d] elected leader for term %d%n", selfId, term);
    }

    private void stepDownIfNewerTerm(long newTerm) {
        synchronized (lock) {
            if (newTerm > currentTerm) {
                currentTerm = newTerm;
                votedFor = null;
                role = Role.FOLLOWER;
                leaderId = null;
                resetElectionDeadline();
                persistState();
                lock.notifyAll(); // wakes any submitAndAwaitCommit call that just lost its leader
            }
        }
    }

    /**
     * Writes term, vote, and apply progress to stable storage. Must be called
     * with {@code lock} held, and must complete before anything observes the
     * state it records — that ordering is the whole point.
     *
     * <p>A failure here is not recoverable in any useful sense: continuing would
     * mean acting on a vote or term the disk doesn't know about, which is
     * exactly the corruption persistence exists to prevent. Better to fail the
     * operation loudly than to proceed unsafely.
     */
    private void persistState() {
        try {
            storage.saveState(currentTerm, votedFor, lastApplied);
            lastCheckpointedApplied = lastApplied;
        } catch (IOException e) {
            throw new PersistenceFailure("could not persist Raft state for node " + selfId, e);
        }
    }

    /** Thrown when Raft state could not be made durable; see {@link #persistState()}. */
    static final class PersistenceFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        PersistenceFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ---- inbound: called from PeerConnectionHandler worker threads ------

    RaftRpc.VoteReply handleRequestVote(long term, int candidateId, int candidateLastLogIndex, long candidateLastLogTerm) {
        synchronized (lock) {
            if (term < currentTerm) {
                return new RaftRpc.VoteReply(currentTerm, false);
            }
            if (term > currentTerm) {
                currentTerm = term;
                votedFor = null;
                role = Role.FOLLOWER;
                lock.notifyAll();
            }
            boolean alreadyVotedForSomeoneElse = votedFor != null && votedFor != candidateId;
            boolean candidateLogOk = isLogAtLeastAsUpToDate(candidateLastLogTerm, candidateLastLogIndex);
            boolean granted = !alreadyVotedForSomeoneElse && candidateLogOk;
            if (granted) {
                votedFor = candidateId;
                // Granting a vote means we just heard from a legitimate participant in
                // this term's election, so give it the full timeout before we compete.
                resetElectionDeadline();
            }
            // Before the reply goes out, not after: a granted vote that isn't on
            // disk is one we could forget and give away again after a restart.
            persistState();
            return new RaftRpc.VoteReply(currentTerm, granted);
        }
    }

    /** Must be called with {@code lock} held. Raft §5.4.1: candidate's log must be at least as fresh as ours. */
    private boolean isLogAtLeastAsUpToDate(long candidateLastLogTerm, int candidateLastLogIndex) {
        int ourLastLogIndex = log.lastIndex();
        long ourLastLogTerm = log.termAt(ourLastLogIndex);
        if (candidateLastLogTerm != ourLastLogTerm) {
            return candidateLastLogTerm > ourLastLogTerm;
        }
        return candidateLastLogIndex >= ourLastLogIndex;
    }

    RaftRpc.AppendReply handleAppendEntries(long term, int leaderIdFromRpc, int prevLogIndex, long prevLogTerm,
            List<RaftLog.Entry> entries, int leaderCommit) {
        synchronized (lock) {
            if (term < currentTerm) {
                return new RaftRpc.AppendReply(currentTerm, false); // stale leader
            }
            boolean termChanged = term > currentTerm;
            if (termChanged) {
                currentTerm = term;
                votedFor = null;
            }
            role = Role.FOLLOWER; // a current-or-newer-term leader outranks a lingering candidate
            leaderId = leaderIdFromRpc;
            resetElectionDeadline();
            lock.notifyAll();

            if (prevLogIndex > 0 && (log.lastIndex() < prevLogIndex || log.termAt(prevLogIndex) != prevLogTerm)) {
                if (termChanged) {
                    persistState();
                }
                return new RaftRpc.AppendReply(currentTerm, false); // log doesn't match at prevLogIndex
            }

            try {
                int index = prevLogIndex;
                for (RaftLog.Entry entry : entries) {
                    index++;
                    if (log.lastIndex() >= index && log.termAt(index) != entry.term()) {
                        log.truncateFrom(index); // conflict: existing entry disagrees, drop it and everything after
                    }
                    if (log.lastIndex() < index) {
                        log.append(entry.term(), entry.command());
                    }
                    // else: we already have this exact entry (term matches) — an idempotent retry, no-op
                }
            } catch (IOException e) {
                throw new PersistenceFailure("could not persist replicated entries on node " + selfId, e);
            }

            if (termChanged) {
                persistState();
            }

            if (leaderCommit > commitIndex) {
                commitIndex = Math.min(leaderCommit, log.lastIndex());
                lock.notifyAll();
            }

            return new RaftRpc.AppendReply(currentTerm, true);
        }
    }

    // ---- applying committed entries to the state machine -----------------

    private void applyLoop() {
        while (running) {
            RaftLog.Entry toApply = null;
            synchronized (lock) {
                if (lastApplied < commitIndex) {
                    toApply = log.get(lastApplied + 1);
                }
            }
            if (toApply == null) {
                sleep(PEER_LOOP_TICK_MILLIS);
                continue;
            }
            apply(toApply.command());
            synchronized (lock) {
                lastApplied++;
                // Checkpoint *after* the apply above, never before: that ordering is
                // what guarantees the recorded value can only lag reality, and a
                // lagging checkpoint just replays a few idempotent entries on restart.
                // The reverse would skip entries that were never actually applied.
                if (lastApplied - lastCheckpointedApplied >= APPLY_CHECKPOINT_INTERVAL) {
                    persistState();
                }
            }
        }
    }

    private void apply(byte[] commandBytes) {
        Command command = Command.decode(commandBytes);
        try {
            if (command.type() == Command.PUT) {
                store.put(command.key(), command.value());
            } else {
                store.delete(command.key());
            }
        } catch (IOException e) {
            // The entry is committed cluster-wide (a majority already has it durably)
            // even though this node's local apply failed; logging and moving on beats
            // killing the apply loop, which would stop every later index from ever
            // applying either.
            System.err.printf("[node %d] failed to apply committed entry: %s%n", selfId, e.getMessage());
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
