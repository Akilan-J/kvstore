package kvstore.raft;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import kvstore.wal.WriteAheadLog;

/**
 * Durable storage for the Raft state the paper requires on stable storage:
 * {@code currentTerm}, {@code votedFor}, and the log.
 *
 * <p><b>Why this has to exist.</b> Without it a node that restarts comes back
 * having forgotten it already voted in the current term, so it can vote a
 * second time — and two votes in one term is exactly how two leaders get
 * elected for the same term. Everything else Raft does rests on that not
 * happening, so the paper's rule is that these are written to stable storage
 * <em>before</em> the node replies to any RPC that depended on them.
 *
 * <p>Two files, both append-only with a CRC per record and torn-tail
 * truncation on recovery, following {@link WriteAheadLog}'s format and for the
 * same reason: a process killed mid-write leaves a partial record, and the
 * only safe thing to do with it is drop it.
 *
 * <pre>
 *   raft.state   | len:4 | term:8 | votedFor:4 | lastApplied:4 | crc:4 |
 *   raft.log     | len:4 | term:8 | cmdLen:4   | command       | crc:4 |
 * </pre>
 *
 * <p>{@code raft.state} is append-only rather than rewritten in place because a
 * rewrite has a window where the file holds neither the old value nor the new
 * one. Appending means the previous record is still intact if the new one tears,
 * and recovery simply takes the last record that passes its CRC. It grows by one
 * ~20-byte record per term change, which is negligible: terms change on
 * elections, not on writes.
 *
 * <p>{@code votedFor} is stored as -1 for "haven't voted this term".
 *
 * <p>Not thread-safe. {@link RaftNode} holds its lock across every call, which
 * is what makes the ordering guarantee meaningful — the fsync completes before
 * the lock is released and a reply goes out.
 */
final class RaftStorage implements Closeable {

    /** currentTerm/votedFor/lastApplied as recovered from disk. */
    record PersistentState(long currentTerm, Integer votedFor, int lastApplied) {}

    private static final int STATE_PAYLOAD_LEN = 8 + 4 + 4;
    private static final int STATE_RECORD_LEN = STATE_PAYLOAD_LEN + 4;
    private static final int MIN_LOG_PAYLOAD_LEN = 8 + 4;
    private static final int MAX_LOG_RECORD_LEN = 64 * 1024 * 1024;

    private final WriteAheadLog.SyncPolicy syncPolicy;

    private final RandomAccessFile stateFile;
    private final FileChannel stateChannel;
    private final RandomAccessFile logFile;
    private final FileChannel logChannel;

    /** Byte offset of each log entry; {@code offsets.get(i)} is the entry at 1-based index i+1. */
    private final List<Long> offsets = new ArrayList<>();

    private long fsyncCount;

    RaftStorage(Path dir, WriteAheadLog.SyncPolicy syncPolicy) throws IOException {
        Files.createDirectories(dir);
        this.syncPolicy = syncPolicy;
        this.stateFile = new RandomAccessFile(dir.resolve("raft.state").toFile(), "rw");
        this.stateChannel = stateFile.getChannel();
        this.logFile = new RandomAccessFile(dir.resolve("raft.log").toFile(), "rw");
        this.logChannel = logFile.getChannel();
    }

    // ---- recovery -------------------------------------------------------

    /**
     * Replays {@code raft.state} and returns the last record that passed its CRC —
     * the one recovery actually uses. Defaults to term 0 with no vote if the file
     * is empty or entirely unreadable, which is the correct state for a node that
     * has never run.
     */
    PersistentState loadState() throws IOException {
        List<PersistentState> history = loadStateHistory();
        return history.isEmpty() ? new PersistentState(0, null, 0) : history.get(history.size() - 1);
    }

    /**
     * Every state record still in the file, oldest first. Recovery only needs the
     * last one, but the full history is what lets a checker verify the node never
     * recorded two different votes in a single term.
     */
    List<PersistentState> loadStateHistory() throws IOException {
        long size = stateChannel.size();
        stateChannel.position(0);
        InputStream in = new BufferedInputStream(Channels.newInputStream(stateChannel), 1 << 16);

        List<PersistentState> history = new ArrayList<>();
        long goodOffset = 0;

        while (goodOffset + 4 + STATE_RECORD_LEN <= size) {
            byte[] lenBytes = readFully(in, 4);
            if (lenBytes == null) {
                break;
            }
            if (ByteBuffer.wrap(lenBytes).getInt() != STATE_RECORD_LEN) {
                break; // not a record we wrote; treat the rest as torn
            }
            byte[] record = readFully(in, STATE_RECORD_LEN);
            if (record == null || !crcOk(record, STATE_PAYLOAD_LEN)) {
                break;
            }
            ByteBuffer bb = ByteBuffer.wrap(record, 0, STATE_PAYLOAD_LEN);
            long term = bb.getLong();
            int votedFor = bb.getInt();
            int lastApplied = bb.getInt();
            history.add(new PersistentState(term, votedFor < 0 ? null : votedFor, lastApplied));
            goodOffset += 4L + STATE_RECORD_LEN;
        }

        truncateTo(stateChannel, goodOffset, size);
        return history;
    }

    /** Replays {@code raft.log}, rebuilding both the entries and their byte offsets. */
    List<RaftLog.Entry> loadLog() throws IOException {
        long size = logChannel.size();
        logChannel.position(0);
        InputStream in = new BufferedInputStream(Channels.newInputStream(logChannel), 1 << 16);

        List<RaftLog.Entry> entries = new ArrayList<>();
        offsets.clear();
        long goodOffset = 0;

        while (true) {
            byte[] lenBytes = readFully(in, 4);
            if (lenBytes == null) {
                break;
            }
            int len = ByteBuffer.wrap(lenBytes).getInt();
            if (len < MIN_LOG_PAYLOAD_LEN + 4 || len > MAX_LOG_RECORD_LEN || goodOffset + 4L + len > size) {
                break; // nonsense length, or the record runs past EOF => torn write
            }
            byte[] record = readFully(in, len);
            if (record == null) {
                break;
            }
            int payloadLen = len - 4;
            if (!crcOk(record, payloadLen)) {
                break;
            }
            ByteBuffer bb = ByteBuffer.wrap(record, 0, payloadLen);
            long term = bb.getLong();
            int cmdLen = bb.getInt();
            if (cmdLen < 0 || cmdLen > bb.remaining()) {
                break;
            }
            byte[] command = new byte[cmdLen];
            bb.get(command);

            offsets.add(goodOffset);
            entries.add(new RaftLog.Entry(term, command));
            goodOffset += 4L + len;
        }

        truncateTo(logChannel, goodOffset, size);
        return entries;
    }

    // ---- writes ---------------------------------------------------------

    /**
     * Records term, vote, and apply progress. Returns only once the bytes are on
     * stable storage under {@link WriteAheadLog.SyncPolicy#EVERY_WRITE} — callers
     * rely on that to satisfy Raft's "persist before replying" rule.
     */
    void saveState(long currentTerm, Integer votedFor, int lastApplied) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(4 + STATE_RECORD_LEN);
        bb.putInt(STATE_RECORD_LEN);
        bb.putLong(currentTerm);
        bb.putInt(votedFor == null ? -1 : votedFor);
        bb.putInt(lastApplied);
        appendCrcAndWrite(stateChannel, bb, STATE_PAYLOAD_LEN);
    }

    /** Appends one entry to the end of the log. */
    void appendEntry(RaftLog.Entry entry) throws IOException {
        long offset = logChannel.size();
        byte[] command = entry.command();
        int payloadLen = 8 + 4 + command.length;
        ByteBuffer bb = ByteBuffer.allocate(4 + payloadLen + 4);
        bb.putInt(payloadLen + 4);
        bb.putLong(entry.term());
        bb.putInt(command.length);
        bb.put(command);
        appendCrcAndWrite(logChannel, bb, payloadLen);
        offsets.add(offset);
    }

    /**
     * Physically drops every entry from 1-based {@code fromIndex} onward, so a
     * follower's log can be forced to match the leader's. Truncating the file
     * rather than marking entries dead keeps recovery honest: what's in the file
     * is exactly the log, with no tombstones to interpret.
     */
    void truncateFrom(int fromIndex) throws IOException {
        if (fromIndex < 1 || fromIndex > offsets.size()) {
            return;
        }
        long cutAt = offsets.get(fromIndex - 1);
        logChannel.truncate(cutAt);
        logChannel.force(true);
        fsyncCount++;
        offsets.subList(fromIndex - 1, offsets.size()).clear();
    }

    long fsyncCount() {
        return fsyncCount;
    }

    @Override
    public void close() throws IOException {
        try {
            stateChannel.force(false);
            logChannel.force(false);
        } finally {
            stateChannel.close();
            stateFile.close();
            logChannel.close();
            logFile.close();
        }
    }

    // ---- helpers --------------------------------------------------------

    private void appendCrcAndWrite(FileChannel channel, ByteBuffer bb, int payloadLen) throws IOException {
        CRC32 crc = new CRC32();
        crc.update(bb.array(), 4, payloadLen);
        bb.putInt((int) crc.getValue());
        bb.flip();
        channel.position(channel.size());
        while (bb.hasRemaining()) {
            channel.write(bb);
        }
        if (syncPolicy == WriteAheadLog.SyncPolicy.EVERY_WRITE) {
            channel.force(false);
            fsyncCount++;
        }
    }

    private static boolean crcOk(byte[] record, int payloadLen) {
        CRC32 crc = new CRC32();
        crc.update(record, 0, payloadLen);
        return (int) crc.getValue() == ByteBuffer.wrap(record).getInt(payloadLen);
    }

    private static void truncateTo(FileChannel channel, long goodOffset, long size) throws IOException {
        if (goodOffset < size) {
            channel.truncate(goodOffset);
            channel.force(true);
        }
        channel.position(goodOffset);
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
