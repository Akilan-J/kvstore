# Durable Key-Value Store

A persistent key-value store written in Java with no external dependencies. Data
lives in memory for fast lookups and is made durable by a write-ahead log that is
replayed on startup, so the store survives process crashes without data loss.

It also replicates. Point it at two peers and it runs as a Raft cluster: leader
election, log replication, quorum-acknowledged writes, and automatic failover
when the leader dies. Replication is opt-in — omit the cluster flags and it is
exactly the single-node store described above.

**Status.** Stage 1 (durable single node) and stage 2 (Raft replication) are
both implemented and tested. Log compaction via snapshotting is the one planned
item still outstanding — see [Roadmap](#roadmap) for that and for the
simplifications that were made deliberately.

---

## Why a write-ahead log

An in-memory map is fast but forgets everything when the process dies. Writing
the whole map to disk on every update would be correct but absurdly slow, because
one changed key rewrites the entire dataset.

A write-ahead log fixes both: every mutation is appended to the end of a file as a
small record, which is a sequential write, and startup replays the file to rebuild
the map. Sequential appends are the cheapest thing a disk does, and replay cost is
proportional to history rather than to dataset size.

The two invariants that make it work:

1. **Log before apply.** The record is appended and (optionally) fsync'd *before*
   the map is mutated. If the order were reversed, a client could read a value the
   log never recorded, and it would silently vanish on restart.
2. **Writes serialize.** All writes take a single lock, so the log order and the
   map state agree. Without it, two threads could append in one order and apply in
   the other, and a replay would produce a different final state than the live map.
   Reads take no lock at all.

---

## Architecture

One node. Client traffic enters on the left; when replication is enabled, a
write detours through Raft before it is applied.

```
              TCP :7379  (clients)          TCP :8379  (peers)
                  │                              │
         ┌────────▼────────┐            ┌────────▼─────────┐
         │  KVServer       │            │  PeerServer      │
         └────────┬────────┘            └────────┬─────────┘
                  │ one worker per conn          │ one worker per peer
      ┌───────────▼───────────┐        ┌─────────▼──────────────┐
      │  ConnectionHandler    │        │ PeerConnectionHandler  │
      └───────────┬───────────┘        └─────────┬──────────────┘
                  │                              │ RequestVote,
       PUT/DEL    │  GET                         │ AppendEntries
                  │                              │
      ┌───────────▼──────────────────────────────▼───────────┐
      │  RaftNode    term · votedFor · role · log            │
      │              commitIndex · nextIndex / matchIndex    │
      │  ┌────────────────────────────────────────────────┐  │
      │  │ apply thread: committed entries, in order      │  │
      │  └──────────────────────┬─────────────────────────┘  │
      └─────────────────────────┼────────────────────────────┘
                                │
      ┌─────────────────────────▼─────────────────┐
      │  KeyValueStore                            │
      │  ┌─────────────────────┐                  │
      │  │ ConcurrentHashMap   │  ← lock-free reads
      │  └─────────────────────┘                  │
      │            ▲                              │
      │            │ apply after append           │
      └────────────┼──────────────────────────────┘
                   │
      ┌────────────▼──────────┐
      │  WriteAheadLog        │  append-only, CRC per record
      └────────────┬──────────┘
                   │
              kvstore.wal on disk
```

Client and peer traffic sit on **separate ports** on purpose: the client wire
protocol never has to reason about Raft messages, and a change to one protocol
cannot accidentally break the other.

| Component | Responsibility |
| --- | --- |
| `wal/WriteAheadLog` | Record encoding, CRC verification, replay, torn-tail truncation |
| `store/KeyValueStore` | Durability ordering, concurrency control, counters |
| `net/ProtocolReader` | Line and length-prefixed framing |
| `net/KVClient` | Client library used by the tools |
| `server/KVServer` | Listen socket, worker pool, lifecycle |
| `server/ConnectionHandler` | Command dispatch, one connection at a time |
| `raft/RaftNode` | Terms, votes, roles, the replicated log, commit index, apply loop |
| `raft/RaftLog` | The log itself: term-tagged entries, conflict truncation |
| `raft/Command` | Encoding of a replicated PUT/DEL |
| `raft/ClusterConfig` | Static membership, quorum arithmetic |
| `raft/PeerServer` · `PeerConnectionHandler` | Inbound peer RPCs |
| `raft/PeerClient` | Outbound peer RPCs, reconnecting on failure |
| `tools/Bench` | Closed-loop load generator with latency percentiles |
| `tools/Workload` | Deterministic write/verify used by the crash tests |
| `tools/ClusterWorkload` | Same, but retries across a cluster; used by the chaos test |

---

## Log record format

```
+--------+------+---------+-----+---------+-------+--------+
| len:4  | ty:1 | klen:4  | key | vlen:4  | value | crc:4  |
+--------+------+---------+-----+---------+-------+--------+
         |<-------------- payload ---------------->|
         |<---------------------- len ------------------->|
```

All integers are big-endian. `len` counts every byte after itself, including the
CRC. `vlen` is `-1` for a delete tombstone, in which case no value bytes follow.
The CRC32 covers the payload only.

**Recovery.** A process killed mid-write leaves a partial record at the tail.
Replay stops at the first record that is short, has a nonsensical length, or fails
its CRC, then truncates the file to the last known-good offset. The log is
therefore always left in a state where every record it contains is complete — a
partial write costs you that one write, never the file.

---

## Replication

Three nodes, one leader. The leader is the only node that accepts writes; it
appends each one to the replicated log, ships it to the followers, and does not
acknowledge the client until a **majority** has stored it. A write that has been
acknowledged has therefore survived onto at least two of three machines, which is
what lets the cluster lose one and keep every acknowledged write.

**Leader election.** Every node runs a randomised 150–300 ms election timer. A
follower that hears nothing from a leader in that window bumps its term and
becomes a candidate. Randomising the timeout is what breaks symmetry: without it
all three nodes would time out together, split the vote, and repeat. The leader
suppresses those timers by sending heartbeats every 50 ms — comfortably inside
the minimum timeout, so a healthy leader is never mistaken for a dead one.

**Log matching.** `AppendEntries` carries the index and term of the entry
immediately preceding the ones being sent. A follower that doesn't have a
matching entry there rejects the request, and the leader retries one index
further back until they agree, then overwrites whatever diverged. This is what
guarantees that two logs agreeing at some index agree on everything before it.

Two rules from the Raft paper carry most of the safety weight, and both are
implemented:

1. **Election restriction (§5.4.1).** A vote is only granted to a candidate whose
   log is at least as up to date as the voter's. Without it, a node with a short
   log could win an election and then truncate entries that were already
   committed elsewhere — silent data loss, not merely a stale replica.
2. **No committing prior terms directly (§5.4.2).** A new leader inherits entries
   from previous terms but may not mark them committed on replica count alone; it
   must first commit an entry of its own term. Skipping this lets an entry that
   looks safely replicated be overwritten later.

Committed entries are handed to a **dedicated apply thread** rather than being
applied inline. Applying means calling into `KeyValueStore`, which does disk I/O,
and that should never block heartbeat or vote traffic — a leader stalled on an
fsync would start losing elections it should win.

**Concurrency.** All Raft state — term, vote, role, log, commit index — is
guarded by one lock, for the same reason `KeyValueStore` serialises writes: so
that two RequestVote RPCs arriving on different connections cannot both be
granted in the same term. Outbound traffic uses one long-lived thread per peer,
each owning its own connection, which is what keeps that connection single-owner
and lock-free.

### What is deliberately simplified

Written down honestly rather than papered over:

- **Raft state is in memory only.** The paper requires term, vote, and log on
  stable storage before replying to any RPC. A node that restarts here comes back
  with no history, so it could vote twice in one term. The chaos test kills nodes
  and leaves them dead, so it never exercises this — but it is a real gap, not a
  cosmetic one, and it is the next thing being built.
- **Reads trust local role.** `GET` is refused unless the node believes it is
  leader, but that belief is not reconfirmed against a quorum. A leader isolated
  by a partition can keep serving reads for up to one election timeout after the
  cluster has replaced it. Real Raft closes this with a read-index round or a
  leader lease; neither is here yet.
- **Membership is fixed at startup.** No joint consensus, no adding or removing
  nodes at runtime.
- **`DEL` always reports `:1` once committed.** Whether the key actually existed
  is not threaded back from the apply loop, unlike the single-node path.
- **Backtracking is one index at a time.** The paper's optional
  skip-by-term optimisation is not implemented; at this scale a follower catches
  up in a handful of heartbeats regardless.

---

## Wire protocol

Text commands, length-prefixed binary values, CRLF-terminated. Deliberately close
in shape to Redis's RESP so the framing is familiar, but not wire-compatible.

| Request | Reply |
| --- | --- |
| `PING` | `+PONG` |
| `GET <key>` | `$<n>` + n bytes, or `$-1` if absent |
| `PUT <key> <nbytes>` + payload | `+OK` |
| `DEL <key>` | `:1` if removed, `:0` if absent |
| `STATS` | `+<counters>` |
| `QUIT` | `+OK`, then close |

Errors return `-ERR <message>`. Values are length-prefixed rather than delimited
so they can contain arbitrary bytes — CR, LF, spaces — with no escaping.

Try it with netcat:

```
$ printf 'PUT greeting 5\r\nhello\r\nGET greeting\r\n' | nc 127.0.0.1 7379
+OK
$5
hello
```

**In cluster mode the protocol is unchanged, but only the leader answers.** Every
command above behaves identically on the leader. On a follower, `GET`, `PUT`, and
`DEL` are all refused with a pointer to the current leader, so a client can find
its way with no separate discovery mechanism:

```
$ printf 'GET greeting\r\n' | nc 127.0.0.1 7379
-ERR not leader (leader is node 3)
```

`PUT` and `DEL` on the leader block until the write is committed by a majority,
so `+OK` means replicated, not merely accepted. If the leader dies mid-write the
client sees an error rather than a false acknowledgement, and can retry against
another node — which is exactly what `ClusterWorkload` does.

---

## Running it

### Single node

```bash
./scripts/build.sh
java -cp build kvstore.Main --port 7379 --data data/kvstore.wal --sync every
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--port` | `7379` | Client listen port |
| `--threads` | `32` | Worker pool size |
| `--data` | `data/kvstore.wal` | Log file path |
| `--sync` | `every` | `every` fsyncs each write; `never` leaves it to the OS |
| `--id` | *(unset)* | This node's Raft id. **Omitting it disables replication entirely** |
| `--raft-port` | `--port` + 1000 | Peer RPC listen port |
| `--peers` | *(empty)* | The other nodes: `id=host:port,id=host:port` |

### Three-node cluster

Each node needs its own client port, peer port, data file, and the peer list of
the *other* two. From three terminals:

```bash
java -cp build kvstore.Main --port 7379 --data data/n1.wal \
    --id 1 --raft-port 8379 --peers "2=127.0.0.1:8380,3=127.0.0.1:8381"

java -cp build kvstore.Main --port 7380 --data data/n2.wal \
    --id 2 --raft-port 8380 --peers "1=127.0.0.1:8379,3=127.0.0.1:8381"

java -cp build kvstore.Main --port 7381 --data data/n3.wal \
    --id 3 --raft-port 8381 --peers "1=127.0.0.1:8379,2=127.0.0.1:8380"
```

Within a few hundred milliseconds one node logs `elected leader for term 1`.
Write to that node's client port; the other two will redirect you to it.

---

## Durability tests

```bash
./scripts/crash_test.sh
```

Four scenarios, eight assertions. The kill is `SIGKILL`, not `SIGTERM` — no
shutdown hook runs and nothing gets flushed on the way out, because a graceful
stop would prove nothing about the log.

| Scenario | Asserts |
| --- | --- |
| 1. `SIGKILL` after 5,000 acknowledged writes | Every write survives; replay count is exact |
| 2. `SIGKILL` after 100 deletes | Deleted keys stay deleted; tombstones are durable |
| 3. 137 random bytes appended to the log | Corrupt tail truncated, prior state intact |
| 4. Log truncated mid-record | Node starts instead of crashing; torn record dropped; writes still work |

Result on the reference run:

```
[1] recovered 5000 records into 5000 keys in 64.5 ms
    verified 5000 keys: 0 missing, 0 mismatched
[2] checked 100 keys: 0 unexpectedly present
[3] corrupt tail truncated back to 446180 bytes
[4] node started cleanly from a torn log; writes still work

8 passed, 0 failed
```

---

## Chaos test

```bash
./scripts/chaos_test.sh
```

The durability tests above prove one node doesn't lose data. This proves the
*cluster* doesn't lose data when a node dies at the worst possible moment.

A background writer hammers a 3-node cluster while the current leader is
`SIGKILL`ed mid-run. The writer is not told that a failover happened — it simply
retries against the next node whenever one rejects it or stops answering, since
from a client's point of view "not the leader" and "dead" both just mean *try
someone else*. Three assertions: the writer finishes every write, a surviving
node takes over, and every key is still there afterwards with the right value.

```
[1] Kill the leader mid-write; assert failover and zero data loss
  initial leader: node 3
  SIGKILLed leader node 3 mid-write
  PASS: writer finished all 300 writes despite the leader dying mid-run
  PASS: a surviving node was elected leader after the kill
  verified 300 keys: 0 missing, 0 mismatched
  PASS: every acknowledged write is present after the failover

3 passed, 0 failed
```

The kill is `SIGKILL` for the same reason it is in the durability tests: no
shutdown hook runs, so the leader gets no chance to hand off gracefully. That is
what an actual machine failure looks like.

---

## Benchmarks

```bash
./scripts/bench.sh 8 3000
```

Reference numbers below are from a **1-vCPU container**, so they are conservative
and the absolute values will be several times higher on real hardware. Re-run on
your own machine before quoting them anywhere. 8 concurrent connections, 64-byte
values, closed-loop.

| Workload | Throughput | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| Write, `--sync every` | 3,840 ops/sec | 1.66 ms | 3.92 ms | 12.07 ms |
| Write, `--sync never` | 15,460 ops/sec | 0.27 ms | 1.42 ms | 5.38 ms |
| Read | 25,815 ops/sec | 0.18 ms | 0.58 ms | 3.51 ms |
| Mixed (10% writes) | 17,604 ops/sec | 0.13 ms | 1.97 ms | 4.49 ms |

**The interesting result is the first two rows.** Turning off fsync makes writes
4× faster, and the entire difference is the cost of waiting for the disk to
confirm. That is the durability/throughput tradeoff made concrete: `--sync every`
survives a machine losing power, `--sync never` only survives the process dying.
The usual production answer is neither — it is *group commit*, where one fsync
covers a batch of waiting writes and amortises the cost across them.

Percentiles are reported rather than means because the mean hides exactly what
matters here: a slow flush stalls one request badly while the rest look fine.

---

## Roadmap

Stage 1 — durable single node:

- [x] Write-ahead log with per-record CRCs and torn-tail truncation
- [x] Crash tests covering `SIGKILL`, tombstones, corruption, and torn writes
- [x] Benchmarks quantifying the fsync tradeoff

Stage 2 — Raft replication:

- [x] Static 3-node cluster membership and a peer RPC channel
- [x] Raft leader election (terms, `RequestVote`, randomised election timeouts)
- [x] Log replication with `AppendEntries` and a commit index
- [x] Reads served only by the leader, writes acknowledged after a quorum
- [x] Chaos test: kill the leader mid-write, assert a new leader is elected and
      no acknowledged write is lost
- [ ] Log compaction via snapshotting, so replay time stops growing forever

Next up:

- [ ] Persist term, vote, and log to disk so a restarted node can safely rejoin
- [ ] Read-index or leader lease, closing the stale-read window described above

Remaining limitations, listed honestly rather than hidden:

- The Raft log lives in memory; a restarted node rejoins with no history.
- The log grows without bound; there is no compaction yet.
- The whole dataset must fit in memory.
- Cluster membership is fixed at startup.
- No authentication or TLS; bind to localhost.
- One lock for all writes, so write throughput will not scale past a few cores.
