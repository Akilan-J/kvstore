package kvstore.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/** Blocking client for the KV wire protocol. One connection, not thread-safe. */
public final class KVClient implements Closeable {

    private final Socket socket;
    private final OutputStream out;
    private final ProtocolReader in;

    public KVClient(String host, int port, int timeoutMillis) throws IOException {
        this.socket = new Socket();
        this.socket.connect(new InetSocketAddress(host, port), timeoutMillis);
        this.socket.setSoTimeout(timeoutMillis);
        this.socket.setTcpNoDelay(true); // don't let Nagle batch our small requests
        this.out = new BufferedOutputStream(socket.getOutputStream(), 1 << 16);
        this.in = new ProtocolReader(new BufferedInputStream(socket.getInputStream(), 1 << 16));
    }

    public KVClient(String host, int port) throws IOException {
        this(host, port, 5000);
    }

    public String ping() throws IOException {
        writeLine("PING");
        return expectSimple();
    }

    public void put(String key, byte[] value) throws IOException {
        out.write(("PUT " + key + " " + value.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.write(value);
        out.write('\r');
        out.write('\n');
        out.flush();
        String reply = expectSimple();
        if (!"OK".equals(reply)) {
            throw new IOException("unexpected PUT reply: " + reply);
        }
    }

    public void put(String key, String value) throws IOException {
        put(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /** @return the value, or null if the key is absent */
    public byte[] get(String key) throws IOException {
        writeLine("GET " + key);
        String header = in.readLine();
        if (header == null) {
            throw new IOException("connection closed during GET");
        }
        if (header.startsWith("-")) {
            throw new IOException("server error: " + header.substring(1));
        }
        if (!header.startsWith("$")) {
            throw new IOException("unexpected GET reply: " + header);
        }
        int len = Integer.parseInt(header.substring(1));
        if (len < 0) {
            return null;
        }
        byte[] value = in.readExactly(len);
        in.consumeTerminator();
        return value;
    }

    public String getString(String key) throws IOException {
        byte[] value = get(key);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    public boolean delete(String key) throws IOException {
        writeLine("DEL " + key);
        String reply = in.readLine();
        if (reply == null) {
            throw new IOException("connection closed during DEL");
        }
        if (reply.startsWith("-")) {
            throw new IOException("server error: " + reply.substring(1));
        }
        if (!reply.startsWith(":")) {
            throw new IOException("unexpected DEL reply: " + reply);
        }
        return "1".equals(reply.substring(1));
    }

    public String stats() throws IOException {
        writeLine("STATS");
        return expectSimple();
    }

    private void writeLine(String line) throws IOException {
        out.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private String expectSimple() throws IOException {
        String reply = in.readLine();
        if (reply == null) {
            throw new IOException("connection closed by server");
        }
        if (reply.startsWith("-")) {
            throw new IOException("server error: " + reply.substring(1));
        }
        if (!reply.startsWith("+")) {
            throw new IOException("unexpected reply: " + reply);
        }
        return reply.substring(1);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
