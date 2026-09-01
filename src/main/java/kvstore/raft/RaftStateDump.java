package kvstore.raft;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kvstore.wal.WriteAheadLog;

/**
 * Prints what a node has on stable storage, and checks the one invariant that
 * makes elections safe: <b>a node never records two different votes in the same
 * term</b>. If that is ever violated, two leaders can be elected for one term.
 *
 * <p>Lives in {@code kvstore.raft} because {@link RaftStorage} is deliberately
 * package-private — the storage format is an implementation detail, and this is
 * the sanctioned window into it rather than a reason to widen its visibility.
 *
 * <p>Reads the current state only; {@code raft.state} keeps every record it ever
 * appended, but recovery uses the last one, so this reports that.
 *
 * <p>usage: RaftStateDump &lt;raft-dir&gt; [&lt;raft-dir&gt; ...]
 */
public final class RaftStateDump {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("usage: RaftStateDump <raft-dir> [<raft-dir> ...]");
            System.exit(2);
        }

        boolean ok = true;

        for (String arg : args) {
            Path dir = Paths.get(arg);
            // NEVER: this is a read-only inspection, so there is nothing to fsync.
            try (RaftStorage storage = new RaftStorage(dir, WriteAheadLog.SyncPolicy.NEVER)) {
                List<RaftStorage.PersistentState> history = storage.loadStateHistory();
                RaftStorage.PersistentState state = history.isEmpty()
                        ? new RaftStorage.PersistentState(0, null, 0)
                        : history.get(history.size() - 1);
                List<RaftLog.Entry> log = storage.loadLog();

                System.out.printf("%s: term=%d votedFor=%s log_entries=%d last_applied=%d (%d state records)%n",
                        dir, state.currentTerm(),
                        state.votedFor() == null ? "none" : state.votedFor(),
                        log.size(), state.lastApplied(), history.size());

                // The invariant, checked across this node's whole recorded history:
                // within one term it must never have named two different candidates.
                Map<Long, Integer> voteForTerm = new HashMap<>();
                long highestTerm = 0;
                for (RaftStorage.PersistentState record : history) {
                    if (record.currentTerm() < highestTerm) {
                        System.out.printf("  VIOLATION: term went backwards, %d after %d%n",
                                record.currentTerm(), highestTerm);
                        ok = false;
                    }
                    highestTerm = Math.max(highestTerm, record.currentTerm());
                    if (record.votedFor() == null) {
                        continue;
                    }
                    Integer previous = voteForTerm.put(record.currentTerm(), record.votedFor());
                    if (previous != null && !previous.equals(record.votedFor())) {
                        System.out.printf("  VIOLATION: recorded votes %d and %d in term %d%n",
                                previous, record.votedFor(), record.currentTerm());
                        ok = false;
                    }
                }
            }
        }

        // Two nodes voting for different candidates in one term is perfectly legal
        // — candidates vote for themselves and lose — so the only thing asserted
        // is that no single node ever contradicted itself.
        System.out.println(ok ? "vote invariant holds" : "VOTE INVARIANT VIOLATED");
        if (!ok) {
            System.exit(1);
        }
    }
}
