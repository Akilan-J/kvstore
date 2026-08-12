package kvstore.tools;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import kvstore.net.KVClient;

/**
 * Closed-loop load generator: N connections each issue M operations back to back
 * and record per-operation latency, then the harness reports throughput and
 * percentiles.
 *
 * <p>Percentiles matter more than the mean here. With fsync on every write, the
 * mean hides the fact that a slow flush stalls one request badly while the others
 * look fine, so p99 is the number that reflects what a client actually feels.
 *
 * <p>usage: Bench &lt;host&gt; &lt;port&gt; &lt;mode&gt; &lt;clients&gt; &lt;opsPerClient&gt; [valueSize]
 * where mode is write, read, or mixed (10% writes).
 */
public final class Bench {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("usage: Bench <host> <port> <write|read|mixed> <clients> <opsPerClient> [valueSize]");
            System.exit(2);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String mode = args[2];
        int clients = Integer.parseInt(args[3]);
        int opsPerClient = Integer.parseInt(args[4]);
        int valueSize = args.length > 5 ? Integer.parseInt(args[5]) : 64;

        int keySpace = Math.max(1, clients * opsPerClient);
        byte[] payload = new byte[valueSize];
        Arrays.fill(payload, (byte) 'x');

        // A read benchmark against an empty store measures misses, not lookups.
        if (!"write".equals(mode)) {
            try (KVClient warm = new KVClient(host, port, 30_000)) {
                for (int i = 0; i < keySpace; i++) {
                    warm.put("bench:" + i, payload);
                }
            }
        }

        long[][] latencies = new long[clients][];
        Thread[] threads = new Thread[clients];
        CountDownLatch ready = new CountDownLatch(clients);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger failures = new AtomicInteger();

        for (int c = 0; c < clients; c++) {
            final int clientId = c;
            threads[c] = new Thread(() -> {
                long[] samples = new long[opsPerClient];
                try (KVClient client = new KVClient(host, port, 30_000)) {
                    client.ping(); // pay connection setup before the timed section
                    ready.countDown();
                    go.await();
                    for (int i = 0; i < opsPerClient; i++) {
                        int k = ThreadLocalRandom.current().nextInt(keySpace);
                        long start = System.nanoTime();
                        switch (mode) {
                            case "write" -> client.put("bench:" + k, payload);
                            case "read" -> client.get("bench:" + k);
                            case "mixed" -> {
                                if (ThreadLocalRandom.current().nextInt(10) == 0) {
                                    client.put("bench:" + k, payload);
                                } else {
                                    client.get("bench:" + k);
                                }
                            }
                            default -> throw new IllegalArgumentException("bad mode: " + mode);
                        }
                        samples[i] = System.nanoTime() - start;
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                    System.err.println("client " + clientId + " failed: " + e);
                    ready.countDown();
                }
                latencies[clientId] = samples;
            }, "bench-" + c);
            threads[c].start();
        }

        ready.await();
        long start = System.nanoTime();
        go.countDown();
        for (Thread t : threads) {
            t.join();
        }
        double elapsedSeconds = (System.nanoTime() - start) / 1e9;

        long[] all = new long[clients * opsPerClient];
        int n = 0;
        for (long[] perClient : latencies) {
            if (perClient == null) {
                continue;
            }
            for (long sample : perClient) {
                if (sample > 0) {
                    all[n++] = sample;
                }
            }
        }
        all = Arrays.copyOf(all, n);
        Arrays.sort(all);

        System.out.printf("%nmode=%s clients=%d ops=%d value=%dB failures=%d%n",
                mode, clients, n, valueSize, failures.get());
        System.out.printf("throughput : %,.0f ops/sec (%.2fs wall)%n", n / elapsedSeconds, elapsedSeconds);
        System.out.printf("latency    : p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms%n",
                percentile(all, 50), percentile(all, 95), percentile(all, 99),
                all.length == 0 ? 0 : all[all.length - 1] / 1e6);
    }

    private static double percentile(long[] sorted, int p) {
        if (sorted.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))] / 1e6;
    }
}
