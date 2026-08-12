package kvstore.store;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import kvstore.wal.WriteAheadLog;

/**
 * A durable in-memory key-value store: the full dataset lives in a hash map, and
 * every mutation is first appended to a write-ahead log so it survives a crash.
 *
 * <p>Concurrency model:
 *
 * <ul>
 *   <li>Writes serialize on {@code writeLock}. This is what makes the log order
 *       and the map state agree — without it, two threads could append in one
 *       order and apply to the map in the other, and a replay would produce a
 *       different final state than the live map.
 *   <li>Reads take no lock. {@link ConcurrentHashMap} gives each reader a
 *       consistent view of a single key, which is all a point lookup needs.
 * </ul>
 *
 * <p>Durability ordering: the log record is appended <em>before</em> the map is
 * mutated. A crash between the two loses nothing — the record replays on startup.
 * The reverse order would be unsafe: a client could read a value that the log
 * never recorded, and it would silently vanish on restart.
 */
public final class KeyValueStore {

    private final ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();
    private final WriteAheadLog wal;
    private final Object writeLock = new Object();

    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();

    public KeyValueStore(WriteAheadLog wal) {
        this.wal = wal;
    }

    /** Rebuilds state from the log. Call once, before serving traffic. */
    public long recover() throws IOException {
        return wal.recover(new WriteAheadLog.RecordVisitor() {
            @Override
            public void onPut(String key, byte[] value) {
                data.put(key, value);
            }

            @Override
            public void onDelete(String key) {
                data.remove(key);
            }
        });
    }

    /** @return the value, or null if the key is absent */
    public byte[] get(String key) {
        gets.incrementAndGet();
        byte[] value = data.get(key);
        if (value != null) {
            hits.incrementAndGet();
        }
        return value;
    }

    public void put(String key, byte[] value) throws IOException {
        synchronized (writeLock) {
            wal.appendPut(key, value); // durable first
            data.put(key, value); // then visible
        }
        puts.incrementAndGet();
    }

    /** @return true if the key existed and was removed */
    public boolean delete(String key) throws IOException {
        synchronized (writeLock) {
            if (!data.containsKey(key)) {
                return false; // nothing to log; a tombstone here would be dead weight
            }
            wal.appendDelete(key);
            data.remove(key);
        }
        deletes.incrementAndGet();
        return true;
    }

    public int size() {
        return data.size();
    }

    public WriteAheadLog wal() {
        return wal;
    }

    public String stats() throws IOException {
        return String.format(
                "keys=%d gets=%d hits=%d puts=%d deletes=%d wal_records=%d wal_bytes=%d wal_file_bytes=%d fsyncs=%d sync_policy=%s",
                data.size(),
                gets.get(),
                hits.get(),
                puts.get(),
                deletes.get(),
                wal.recordsAppended(),
                wal.bytesAppended(),
                wal.sizeOnDisk(),
                wal.fsyncCount(),
                wal.syncPolicy());
    }
}
