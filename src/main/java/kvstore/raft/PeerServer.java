package kvstore.raft;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Listens for inbound Raft RPC connections from peers.
 *
 * <p>Deliberately a separate listener from {@code kvstore.server.KVServer}'s
 * client port: keeping client traffic and peer traffic on different ports
 * means the client wire protocol never has to reason about Raft messages, and
 * a change to one protocol can't accidentally break the other.
 *
 * <p>Structurally this mirrors KVServer (accept loop handing connections to a
 * fixed pool), but the pool is sized to the number of peers rather than a
 * large constant — membership is static and small, so there's no unbounded-
 * client risk to bound against here, just one worker per peer connection.
 */
public final class PeerServer {

    private final int port;
    private final int poolSize;
    private final RaftNode raftNode;

    private ServerSocket serverSocket;
    private ExecutorService pool;
    private Thread acceptThread;
    private volatile boolean running;

    public PeerServer(int port, int poolSize, RaftNode raftNode) {
        this.port = port;
        this.poolSize = poolSize;
        this.raftNode = raftNode;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port), 512);

        AtomicInteger workerId = new AtomicInteger();
        pool = Executors.newFixedThreadPool(poolSize, runnable -> {
            Thread t = new Thread(runnable, "raft-peer-" + workerId.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        running = true;
        acceptThread = new Thread(this::acceptLoop, "raft-accept");
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket peer = serverSocket.accept();
                try {
                    pool.execute(new PeerConnectionHandler(peer, raftNode));
                } catch (RejectedExecutionException e) {
                    peer.close(); // shutting down
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("peer accept failed: " + e.getMessage());
                }
                return;
            }
        }
    }

    public int port() {
        return serverSocket == null ? port : serverSocket.getLocalPort();
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // closing the listen socket is what breaks the accept loop
        }
        if (pool != null) {
            pool.shutdownNow();
            try {
                pool.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
