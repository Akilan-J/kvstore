package kvstore.raft;

/** Wire-level reply types for the peer RPCs implemented so far. */
final class RaftRpc {

    private RaftRpc() {}

    record VoteReply(long term, boolean granted) {}

    /**
     * {@code success} is false when the follower's log didn't match at
     * {@code prevLogIndex}/{@code prevLogTerm} (or its term was stale) — the
     * leader responds by backing off {@code nextIndex} and retrying.
     */
    record AppendReply(long term, boolean success) {}
}
