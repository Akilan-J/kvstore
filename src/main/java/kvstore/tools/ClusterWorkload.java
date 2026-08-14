package kvstore.tools;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import kvstore.net.KVClient;

/**
 * Drives writes and verifies reads against a Raft cluster rather than a single
 * node, retrying against a different node whenever the one it's using rejects a
 * request (wrong leader) or goes unreachable (dead), instead of giving up.
 *
 * <p>Used by the chaos test: the write phase runs while the current leader gets
 * SIGKILLed partway through. It keeps retrying each key — against the rest of
 * the cluster — until it succeeds, simulating a client that doesn't give up,
 * rather than trying to track exactly which writes landed before the kill. The
 * verify phase then confirms every key is present with the correct value once
 * the dust settles.
 *
 * <p>One connection is reused across calls for as long as it keeps working, and
 * only replaced on failure — the common case (many consecutive writes to the
 * same stable leader) doesn't pay a reconnect cost, only the handful of writes
 * caught near the actual leader transition do.
 *
 * <p>usage: ClusterWorkload &lt;write|verify&gt; &lt;count&gt; &lt;valueSize&gt; &lt;host:port&gt; [host:port ...]
 */
public final class ClusterWorkload {

    private static final int ATTEMPT_TIMEOUT_MILLIS = 300;
    // Generous on purpose: real leader transitions finish in well under a second
    // (election timeout is 150-300ms), so hitting this is a sign of a real bug,
    // not a slow-but-healthy cluster.
    private static final long PER_KEY_DEADLINE_MILLIS = 10_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("usage: ClusterWorkload <write|verify> <count> <valueSize> <host:port> [host:port ...]");
            System.exit(2);
        }
        String mode = args[0];
        int count = Integer.parseInt(args[1]);
        int valueSize = Integer.parseInt(args[2]);

        List<InetSocketAddress> nodes = new ArrayList<>();
        for (int i = 3; i < args.length; i++) {
            int colon = args[i].lastIndexOf(':');
            nodes.add(new InetSocketAddress(args[i].substring(0, colon), Integer.parseInt(args[i].substring(colon + 1))));
        }

        Cluster cluster = new Cluster(nodes);
        try {
            switch (mode) {
                case "write" -> {
                    for (int i = 0; i < count; i++) {
                        String key = key(i);
                        byte[] value = value(i, valueSize);
                        cluster.call(client -> {
                            client.put(key, value);
                            return null;
                        });
                    }
                    System.out.println("wrote " + count + " keys");
                }
                case "verify" -> {
                    int missing = 0;
                    int mismatched = 0;
                    for (int i = 0; i < count; i++) {
                        String key = key(i);
                        byte[] expected = value(i, valueSize);
                        byte[] actual = cluster.call(client -> client.get(key));
                        if (actual == null) {
                            missing++;
                            if (missing <= 3) {
                                System.out.println("  MISSING " + key);
                            }
                        } else if (!Arrays.equals(actual, expected)) {
                            mismatched++;
                            if (mismatched <= 3) {
                                System.out.println("  MISMATCH " + key);
                            }
                        }
                    }
                    System.out.printf("verified %d keys: %d missing, %d mismatched%n", count, missing, mismatched);
                    if (missing > 0 || mismatched > 0) {
                        System.exit(1);
                    }
                }
                default -> {
                    System.err.println("unknown mode: " + mode);
                    System.exit(2);
                }
            }
        } finally {
            cluster.close();
        }
    }

    @FunctionalInterface
    private interface ClusterOp<T> {
        T run(KVClient client) throws IOException;
    }

    /** Owns the one active connection and which node it points at. */
    private static final class Cluster {
        private final List<InetSocketAddress> nodes;
        private int current = 0;
        private KVClient client;

        Cluster(List<InetSocketAddress> nodes) {
            this.nodes = nodes;
        }

        /**
         * Tries {@code op} against the active connection, moving to the next node on
         * any failure — a rejection (wrong leader) and a dead node are handled
         * identically, since both just mean "try someone else" — until it succeeds
         * or {@code PER_KEY_DEADLINE_MILLIS} elapses.
         */
        <T> T call(ClusterOp<T> op) throws IOException {
            long deadline = System.nanoTime() + PER_KEY_DEADLINE_MILLIS * 1_000_000L;
            IOException lastError = null;
            while (System.nanoTime() < deadline) {
                try {
                    if (client == null) {
                        InetSocketAddress addr = nodes.get(current);
                        client = new KVClient(addr.getHostString(), addr.getPort(), ATTEMPT_TIMEOUT_MILLIS);
                    }
                    return op.run(client);
                } catch (IOException e) {
                    lastError = e;
                    close();
                    current = (current + 1) % nodes.size();
                }
            }
            throw new IOException("gave up after " + PER_KEY_DEADLINE_MILLIS + "ms; last error: "
                    + (lastError == null ? "none" : lastError.getMessage()));
        }

        void close() {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException ignored) {
                    // discarding this connection either way
                }
                client = null;
            }
        }
    }

    private static String key(int i) {
        return "chaos:" + i;
    }

    /** Deterministic value so the verify phase can recompute what it expects. */
    private static byte[] value(int i, int size) {
        byte[] value = new byte[size];
        for (int j = 0; j < size; j++) {
            value[j] = (byte) ((i * 31 + j) & 0xFF);
        }
        return value;
    }
}
