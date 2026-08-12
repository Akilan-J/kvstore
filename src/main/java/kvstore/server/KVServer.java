package kvstore.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import kvstore.store.KeyValueStore;

/**
 * Thread-per-connection TCP server backed by a bounded pool.
 *
 * <p>Bounded rather than unbounded ({@code newCachedThreadPool}) so that N
 * concurrent clients cannot spawn N OS threads. The cost is that connection
 * number {@code poolSize + 1} waits in the queue rather than being served, which
 * is the right tradeoff for a store where connections are long-lived and few.
 */
public final class KVServer {

    private final int port;
    private final int poolSize;
    private final KeyValueStore store;

    private ServerSocket serverSocket;
    private ExecutorService pool;
    private Thread acceptThread;
    private volatile boolean running;

    public KVServer(int port, int poolSize, KeyValueStore store) {
        this.port = port;
        this.poolSize = poolSize;
        this.store = store;
    }

    /** Binds the listen socket and starts accepting in a background thread. */
    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port), 512);

        AtomicInteger workerId = new AtomicInteger();
        pool = Executors.newFixedThreadPool(poolSize, runnable -> {
            Thread t = new Thread(runnable, "kv-worker-" + workerId.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        running = true;
        acceptThread = new Thread(this::acceptLoop, "kv-accept");
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                try {
                    pool.execute(new ConnectionHandler(client, store));
                } catch (RejectedExecutionException e) {
                    client.close(); // shutting down
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("accept failed: " + e.getMessage());
                }
                return;
            }
        }
    }

    public int port() {
        return serverSocket == null ? port : serverSocket.getLocalPort();
    }

    public void awaitTermination() throws InterruptedException {
        acceptThread.join();
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
