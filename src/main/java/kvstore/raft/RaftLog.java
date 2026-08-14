package kvstore.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * The replicated log: term-tagged, opaque command bytes, 1-indexed so that
 * index 0 can mean "before the first entry" — the Raft paper's sentinel for
 * an empty {@code prevLogIndex}.
 *
 * <p>In-memory only. A node that restarts loses its whole log, which is
 * unsafe in general (see {@link RaftNode}'s class comment) but out of scope
 * here — see CLAUDE.md's known limitations.
 *
 * <p>Not thread-safe by itself; {@link RaftNode} holds its lock for every
 * access, same as term/vote/role.
 */
final class RaftLog {

    record Entry(long term, byte[] command) {}

    private final List<Entry> entries = new ArrayList<>(); // entries.get(i) is 1-based index i+1

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

    int append(long term, byte[] command) {
        entries.add(new Entry(term, command));
        return entries.size();
    }

    /** Deletes every entry from {@code fromIndex} (1-based, inclusive) onward. */
    void truncateFrom(int fromIndex) {
        if (fromIndex <= entries.size()) {
            entries.subList(Math.max(fromIndex, 1) - 1, entries.size()).clear();
        }
    }
}
