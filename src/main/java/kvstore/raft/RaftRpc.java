package kvstore.raft;

/** Wire-level reply types for the peer RPCs implemented so far. */
final class RaftRpc {

    private RaftRpc() {}

    record VoteReply(long term, boolean granted) {}

    /** No log entries yet — AppendEntries is purely the leader's heartbeat until log replication lands. */
    record AppendReply(long term) {}
}
