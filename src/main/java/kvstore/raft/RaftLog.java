package kvstore.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The replicated log: term-tagged, opaque command bytes, 1-indexed so that
 * index 0 can mean "before the first entry" — the Raft paper's sentinel for
 * an empty {@code prevLogIndex}.
 *
 * <p>Entries are held in memory for lookup and written through to
 * {@link RaftStorage} so they survive a restart. The in-memory list is the
 * read path; the file is the truth. Both are mutated under {@link RaftNode}'s
 * lock, and the disk write happens first — same ordering rule as
 * {@code KeyValueStore.put}, and for the same reason: never let anything
 * observe state the durable record doesn't contain.
 *
 * <p>Not thread-safe by itself; {@link RaftNode} holds its lock for every
 * access, same as term/vote/role.
 */
final class RaftLog {

    record Entry(long term, byte[] command) {}

    private final List<Entry> entries = new ArrayList<>(); // entries.get(i) is 1-based index i+1
    private final RaftStorage storage;

    RaftLog(RaftStorage storage) {
        this.storage = storage;
    }

    /** Seeds the in-memory list from what recovery read off disk. */
    void restore(List<Entry> recovered) {
        entries.clear();
        entries.addAll(recovered);
    }

    /** Highest index in the log, or 0 if empty. */
    int lastIndex() {
        return entries.size();
    }

    /** Term of the entry at a 1-based index, or 0 for index 0 or an out-of-range index. */
    long termAt(int index) {
        if (index <= 0 || index > entries.size()) {
            return 0;
        }
        return entries.get(index - 1).term();
    }

    Entry get(int index) {
        return entries.get(index - 1);
    }

    /** Entries at indexes {@code [fromIndex, lastIndex()]}, in order; empty if fromIndex &gt; lastIndex(). */
    List<Entry> entriesFrom(int fromIndex) {
        if (fromIndex > entries.size()) {
            return List.of();
        }
        return List.copyOf(entries.subList(Math.max(fromIndex, 1) - 1, entries.size()));
    }

    int append(long term, byte[] command) throws IOException {
        Entry entry = new Entry(term, command);
        storage.appendEntry(entry); // durable before visible
        entries.add(entry);
        return entries.size();
    }

    /** Deletes every entry from {@code fromIndex} (1-based, inclusive) onward. */
    void truncateFrom(int fromIndex) throws IOException {
        if (fromIndex <= entries.size()) {
            storage.truncateFrom(fromIndex);
            entries.subList(Math.max(fromIndex, 1) - 1, entries.size()).clear();
        }
    }
}
