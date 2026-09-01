package kvstore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import kvstore.raft.ClusterConfig;
import kvstore.raft.PeerClient;
import kvstore.raft.PeerServer;
import kvstore.raft.RaftNode;
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
        Integer raftId = null; // null means Raft is disabled: plain stage-1 node
        int raftPort = -1;
        String peersArg = "";
        Path raftDir = null;

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
                case "--id" -> raftId = Integer.parseInt(args[++i]);
                case "--raft-port" -> raftPort = Integer.parseInt(args[++i]);
                case "--peers" -> peersArg = args[++i];
                case "--raft-dir" -> raftDir = Paths.get(args[++i]);
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

        RaftNode raftNode = null;
        ClusterConfig cluster = null;
        if (raftId != null) {
            if (raftPort < 0) {
                raftPort = port + 1000; // keeps client and peer ports from colliding by default
            }
            cluster = ClusterConfig.parse(raftId, peersArg);

            Map<Integer, PeerClient> peerClients = new LinkedHashMap<>();
            for (Map.Entry<Integer, InetSocketAddress> e : cluster.peers().entrySet()) {
                // Timeout well under the minimum election timeout, so one slow or dead
                // peer can't stall the loop that's supposed to be detecting it as dead.
                peerClients.put(e.getKey(), new PeerClient(e.getValue(), 100));
            }
            if (raftDir == null) {
                // Default beside the store's own log, so one --data path is enough to
                // give a node everything it owns on disk.
                Path parent = dataFile.toAbsolutePath().getParent();
                raftDir = (parent == null ? Paths.get(".") : parent).resolve("raft");
            }
            raftNode = new RaftNode(raftId, cluster, peerClients, store, raftDir, sync);
        }

        // KVServer needs raftNode (possibly null) at construction, so it's built
        // after RaftNode exists but before RaftNode/PeerServer actually start —
        // client connections won't arrive before this method returns anyway.
        KVServer server = new KVServer(port, threads, store, raftNode);
        server.start();

        PeerServer peerServer = null;
        if (raftId != null) {
            // At least 1: newFixedThreadPool(0) would accept connections and then
            // never service them, since no thread exists to run the handlers.
            peerServer = new PeerServer(raftPort, Math.max(1, cluster.peers().size()), raftNode);
            peerServer.start();
            System.out.printf("raft node id=%d cluster_size=%d peer_port=%d dir=%s%n",
                    raftId, cluster.clusterSize(), peerServer.port(), raftDir);
            System.out.printf("raft recovered %s%n", raftNode.recoveredSummary());
            raftNode.start();
        }

        PeerServer finalPeerServer = peerServer;
        RaftNode finalRaftNode = raftNode;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            if (finalRaftNode != null) {
                finalRaftNode.stop();
            }
            if (finalPeerServer != null) {
                finalPeerServer.stop();
            }
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
                  --id <n>            this node's Raft id; enables the Raft peer listener (default: disabled)
                  --raft-port <n>     peer RPC listen port (default: client port + 1000)
                  --peers <list>      other nodes' peer ports: id=host:port,id=host:port,...
                  --raft-dir <path>   directory for raft.state and raft.log (default: <data dir>/raft)
                """);
    }
}
