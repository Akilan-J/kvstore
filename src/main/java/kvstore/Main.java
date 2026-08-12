package kvstore;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import kvstore.server.KVServer;
import kvstore.store.KeyValueStore;
import kvstore.wal.WriteAheadLog;

/** Command-line entry point for a single store node. */
public final class Main {

    public static void main(String[] args) throws Exception {
        int port = 7379;
        int threads = 32;
        Path dataFile = Paths.get("data", "kvstore.wal");
        WriteAheadLog.SyncPolicy sync = WriteAheadLog.SyncPolicy.EVERY_WRITE;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--data" -> dataFile = Paths.get(args[++i]);
                case "--sync" -> sync = switch (args[++i]) {
                    case "every" -> WriteAheadLog.SyncPolicy.EVERY_WRITE;
                    case "never" -> WriteAheadLog.SyncPolicy.NEVER;
                    default -> throw new IllegalArgumentException("--sync must be 'every' or 'never'");
                };
                case "--help" -> {
                    printUsage();
                    return;
                }
                default -> {
                    System.err.println("unknown argument: " + args[i]);
                    printUsage();
                    System.exit(2);
                }
            }
        }

        WriteAheadLog wal = new WriteAheadLog(dataFile, sync);
        KeyValueStore store = new KeyValueStore(wal);

        long start = System.nanoTime();
        long replayed = store.recover();
        double elapsedMillis = (System.nanoTime() - start) / 1e6;
        System.out.printf(
                "recovered %d records into %d keys in %.1f ms from %s%n",
                replayed, store.size(), elapsedMillis, wal.path());

        KVServer server = new KVServer(port, threads, store);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            try {
                wal.close();
            } catch (IOException e) {
                System.err.println("error closing log: " + e.getMessage());
            }
        }));

        // Printed last and flushed so test scripts can wait on a single token.
        System.out.printf("READY port=%d threads=%d sync=%s%n", server.port(), threads, sync);
        System.out.flush();

        server.awaitTermination();
    }

    private static void printUsage() {
        System.out.println("""
                usage: java kvstore.Main [options]
                  --port <n>          listen port (default 7379)
                  --threads <n>       worker pool size (default 32)
                  --data <path>       write-ahead log file (default data/kvstore.wal)
                  --sync every|never  fsync on every write, or leave it to the OS (default every)
                """);
    }
}
