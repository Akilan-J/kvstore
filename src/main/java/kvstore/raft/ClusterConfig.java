package kvstore.raft;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Static Raft cluster membership: this node's id, and the peer-RPC address of
 * every other node. Membership is fixed at startup and never changes at
 * runtime — that's a stage-2 simplification (see CLAUDE.md), not a bug; Raft's
 * own membership-change protocol is out of scope here.
 */
public final class ClusterConfig {

    private final int selfId;
    private final Map<Integer, InetSocketAddress> peers;

    public ClusterConfig(int selfId, Map<Integer, InetSocketAddress> peers) {
        if (peers.containsKey(selfId)) {
            throw new IllegalArgumentException("--peers must not list this node's own id (" + selfId + ")");
        }
        this.selfId = selfId;
        this.peers = Collections.unmodifiableMap(new LinkedHashMap<>(peers));
    }

    /**
     * Parses {@code --peers} of the form {@code id=host:port,id=host:port,...}.
     * Each address is that peer's Raft port, not its client port — the two are
     * deliberately separate listeners (see {@link PeerServer}).
     */
    public static ClusterConfig parse(int selfId, String peersArg) {
        Map<Integer, InetSocketAddress> peers = new LinkedHashMap<>();
        if (peersArg != null && !peersArg.isBlank()) {
            for (String entry : peersArg.split(",")) {
                int eq = entry.indexOf('=');
                int colon = entry.lastIndexOf(':');
                if (eq < 0 || colon < eq) {
                    throw new IllegalArgumentException("bad --peers entry '" + entry + "', expected id=host:port");
                }
                int id = Integer.parseInt(entry.substring(0, eq).trim());
                String host = entry.substring(eq + 1, colon).trim();
                int port = Integer.parseInt(entry.substring(colon + 1).trim());
                peers.put(id, new InetSocketAddress(host, port));
            }
        }
        return new ClusterConfig(selfId, peers);
    }

    public int selfId() {
        return selfId;
    }

    public Map<Integer, InetSocketAddress> peers() {
        return peers;
    }

    /** Cluster size including this node — used for majority-quorum math once election lands. */
    public int clusterSize() {
        return peers.size() + 1;
    }

    /** Votes or acks needed for a majority, counting this node's own. */
    public int quorumSize() {
        return clusterSize() / 2 + 1;
    }
}
