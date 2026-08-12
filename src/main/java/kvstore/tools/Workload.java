package kvstore.tools;

import java.util.Arrays;

import kvstore.net.KVClient;

/**
 * Writes or verifies a deterministic dataset. Used by the crash test: the write
 * phase runs, the server is killed with SIGKILL, and the verify phase then checks
 * that every acknowledged write survived.
 *
 * <p>usage: Workload &lt;write|verify|verify-absent&gt; &lt;host&gt; &lt;port&gt; &lt;count&gt; [valueSize]
 */
public final class Workload {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("usage: Workload <write|verify|verify-absent> <host> <port> <count> [valueSize]");
            System.exit(2);
        }
        String mode = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        int count = Integer.parseInt(args[3]);
        int valueSize = args.length > 4 ? Integer.parseInt(args[4]) : 64;

        try (KVClient client = new KVClient(host, port, 10_000)) {
            switch (mode) {
                case "write" -> {
                    for (int i = 0; i < count; i++) {
                        client.put(key(i), value(i, valueSize));
                    }
                    System.out.println("wrote " + count + " keys");
                }
                case "verify" -> {
                    int missing = 0;
                    int mismatched = 0;
                    for (int i = 0; i < count; i++) {
                        byte[] actual = client.get(key(i));
                        if (actual == null) {
                            missing++;
                            if (missing <= 3) {
                                System.out.println("  MISSING " + key(i));
                            }
                        } else if (!Arrays.equals(actual, value(i, valueSize))) {
                            mismatched++;
                            if (mismatched <= 3) {
                                System.out.println("  MISMATCH " + key(i));
                            }
                        }
                    }
                    System.out.printf("verified %d keys: %d missing, %d mismatched%n", count, missing, mismatched);
                    if (missing > 0 || mismatched > 0) {
                        System.exit(1);
                    }
                }
                case "verify-absent" -> {
                    int present = 0;
                    for (int i = 0; i < count; i++) {
                        if (client.get(key(i)) != null) {
                            present++;
                        }
                    }
                    System.out.printf("checked %d keys: %d unexpectedly present%n", count, present);
                    if (present > 0) {
                        System.exit(1);
                    }
                }
                default -> {
                    System.err.println("unknown mode: " + mode);
                    System.exit(2);
                }
            }
        }
    }

    private static String key(int i) {
        return "key:" + i;
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
