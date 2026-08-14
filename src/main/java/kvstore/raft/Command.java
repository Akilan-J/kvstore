package kvstore.raft;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A replicated write: a PUT with its value, or a DELETE. This is what a
 * {@link RaftLog.Entry}'s command bytes decode to once applied.
 *
 * <p>Deliberately its own encoding rather than reusing
 * {@code WriteAheadLog}'s on-disk record format, even though the shape is
 * similar (type + length-prefixed key + length-prefixed value): this one
 * only ever travels over a single TCP connection between two processes in
 * the same run, so it doesn't need a CRC or its own length-prefix framing —
 * the peer wire protocol's length-prefixed payload already provides that.
 */
record Command(byte type, String key, byte[] value) {

    static final byte PUT = 1;
    static final byte DELETE = 2;

    static Command put(String key, byte[] value) {
        return new Command(PUT, key, value);
    }

    static Command delete(String key) {
        return new Command(DELETE, key, null);
    }

    byte[] encode() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int valLen = (type == PUT) ? value.length : -1;
        ByteBuffer bb = ByteBuffer.allocate(1 + 4 + keyBytes.length + 4 + Math.max(valLen, 0));
        bb.put(type);
        bb.putInt(keyBytes.length);
        bb.put(keyBytes);
        bb.putInt(valLen);
        if (valLen > 0) {
            bb.put(value);
        }
        return bb.array();
    }

    static Command decode(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        byte type = bb.get();
        byte[] keyBytes = new byte[bb.getInt()];
        bb.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        int valLen = bb.getInt();
        byte[] value = null;
        if (valLen >= 0) {
            value = new byte[valLen];
            bb.get(value);
        }
        return new Command(type, key, value);
    }
}
