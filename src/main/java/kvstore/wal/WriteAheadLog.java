package kvstore.wal;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

/**
 * Append-only write-ahead log.
 *
 * <p>Record layout on disk (all integers big-endian):
 *
 * <pre>
 *   +--------+------+---------+-----+---------+-------+--------+
 *   | len:4  | ty:1 | klen:4  | key | vlen:4  | value | crc:4  |
 *   +--------+------+---------+-----+---------+-------+--------+
 *            |&lt;-------------- payload ---------------&gt;|
 *            |&lt;----------------- len ------------------------&gt;|
 * </pre>
 *
 * <p>{@code len} counts every byte after itself, including the trailing CRC.
 * {@code vlen} is -1 for a delete (tombstone) record, in which case no value
 * bytes follow. The CRC32 is computed over the payload only.
 *
 * <p>Durability contract: a record is considered committed once {@link #append}
 * returns. Under {@link SyncPolicy#EVERY_WRITE} that means the bytes have been
 * fsync'd; under {@link SyncPolicy#NEVER} it only means they reached the OS page
 * cache, so a machine crash (not a process crash) can lose them.
 *
 * <p>Crash safety: a process can die mid-write and leave a partial record at the
 * tail of the file. {@link #recover} stops at the first record that is short or
 * fails its CRC and truncates the file to the last known-good offset, so the log
 * is always left in a state where every record it contains is complete.
 */
public final class WriteAheadLog implements Closeable {

    /** fsync on every append (durable, slower) vs. let the OS decide (faster). */
    public enum SyncPolicy {
        EVERY_WRITE,
        NEVER
    }

    public static final byte TYPE_PUT = 1;
    public static final byte TYPE_DELETE = 2;

    /** Smallest legal payload: type + klen + vlen + crc, with an empty key. */
    private static final int MIN_RECORD_LEN = 1 + 4 + 4 + 4;

    /** Guards against a corrupt length field causing a huge allocation. */
    private static final int MAX_RECORD_LEN = 64 * 1024 * 1024;

    /** Callback interface used during replay. */
    public interface RecordVisitor {
        void onPut(String key, byte[] value);

        void onDelete(String key);
    }

    private final Path path;
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final SyncPolicy syncPolicy;

    private long recordsAppended;
    private long bytesAppended;
    private long fsyncCount;

    public WriteAheadLog(Path path, SyncPolicy syncPolicy) throws IOException {
        this.path = path.toAbsolutePath();
        Path parent = this.path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        this.file = new RandomAccessFile(this.path.toFile(), "rw");
        this.channel = file.getChannel();
        this.syncPolicy = syncPolicy;
    }

    /**
     * Replays the log into {@code visitor} in write order, then truncates any
     * torn or corrupt tail.
     *
     * @return the number of records successfully applied
     */
    public long recover(RecordVisitor visitor) throws IOException {
        long size = channel.size();
        channel.position(0);

        InputStream in = new BufferedInputStream(Channels.newInputStream(channel), 1 << 16);
        long goodOffset = 0;
        long applied = 0;

        while (true) {
            byte[] lenBytes = readFully(in, 4);
            if (lenBytes == null) {
                break; // clean EOF, or a tail too short to hold a length field
            }
            int len = ByteBuffer.wrap(lenBytes).getInt();
            if (len < MIN_RECORD_LEN || len > MAX_RECORD_LEN || goodOffset + 4L + len > size) {
                break; // nonsense length, or record extends past EOF => torn write
            }

            byte[] record = readFully(in, len);
            if (record == null) {
                break;
            }

            int payloadLen = len - 4;
            CRC32 crc = new CRC32();
            crc.update(record, 0, payloadLen);
            int expected = ByteBuffer.wrap(record).getInt(payloadLen);
            if ((int) crc.getValue() != expected) {
                break; // partially flushed or bit-rotted record
            }

            ByteBuffer bb = ByteBuffer.wrap(record, 0, payloadLen);
            byte type = bb.get();
            int keyLen = bb.getInt();
            if (keyLen < 0 || keyLen > bb.remaining()) {
                break;
            }
            byte[] keyBytes = new byte[keyLen];
            bb.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            if (bb.remaining() < 4) {
                break;
            }
            int valLen = bb.getInt();

            if (type == TYPE_PUT) {
                if (valLen < 0 || valLen > bb.remaining()) {
                    break;
                }
                byte[] value = new byte[valLen];
                bb.get(value);
                visitor.onPut(key, value);
            } else if (type == TYPE_DELETE) {
                visitor.onDelete(key);
            } else {
                break; // unknown record type
            }

            goodOffset += 4L + len;
            applied++;
        }

        if (goodOffset < size) {
            channel.truncate(goodOffset);
            channel.force(true);
        }
        channel.position(goodOffset);
        return applied;
    }

    public synchronized void appendPut(String key, byte[] value) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null for a PUT");
        }
        append(TYPE_PUT, key.getBytes(StandardCharsets.UTF_8), value);
    }

    public synchronized void appendDelete(String key) throws IOException {
        append(TYPE_DELETE, key.getBytes(StandardCharsets.UTF_8), null);
    }

    private void append(byte type, byte[] key, byte[] value) throws IOException {
        int valLen = (value == null) ? -1 : value.length;
        int valBytes = (valLen < 0) ? 0 : valLen;
        int payloadLen = 1 + 4 + key.length + 4 + valBytes;
        int len = payloadLen + 4;

        ByteBuffer bb = ByteBuffer.allocate(4 + len);
        bb.putInt(len);
        bb.put(type);
        bb.putInt(key.length);
        bb.put(key);
        bb.putInt(valLen);
        if (valBytes > 0) {
            bb.put(value);
        }

        CRC32 crc = new CRC32();
        crc.update(bb.array(), 4, payloadLen);
        bb.putInt((int) crc.getValue());

        bb.flip();
        while (bb.hasRemaining()) {
            channel.write(bb);
        }
        if (syncPolicy == SyncPolicy.EVERY_WRITE) {
            channel.force(false);
            fsyncCount++;
        }
        recordsAppended++;
        bytesAppended += 4L + len;
    }

    /** Forces any buffered writes to stable storage. */
    public synchronized void sync() throws IOException {
        channel.force(false);
        fsyncCount++;
    }

    public long sizeOnDisk() throws IOException {
        return channel.size();
    }

    public long recordsAppended() {
        return recordsAppended;
    }

    public long bytesAppended() {
        return bytesAppended;
    }

    public long fsyncCount() {
        return fsyncCount;
    }

    public Path path() {
        return path;
    }

    public SyncPolicy syncPolicy() {
        return syncPolicy;
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            channel.force(false);
        } finally {
            channel.close();
            file.close();
        }
    }

    /** Reads exactly {@code n} bytes, or returns null if the stream ends first. */
    private static byte[] readFully(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            int read = in.read(buf, off, n - off);
            if (read < 0) {
                return null;
            }
            off += read;
        }
        return buf;
    }
}
