# Durable Key-Value Store

A persistent key-value store written in Java with no external dependencies. Data
lives in memory for fast lookups and is made durable by a write-ahead log that is
replayed on startup, so the store survives process crashes without data loss.

**Stage 1 of 2.** This is the single-node storage engine. Raft replication and
leader failover are the next stage — see [Roadmap](#roadmap).

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

```
              TCP :7379
                  │
         ┌────────▼────────┐
         │  KVServer       │  accept loop + bounded worker pool
         └────────┬────────┘
                  │ one worker per connection
      ┌───────────▼───────────┐
      │  ConnectionHandler    │  parses the wire protocol
      └───────────┬───────────┘
                  │
      ┌───────────▼───────────────────────────────┐
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

| Component | Responsibility |
| --- | --- |
| `wal/WriteAheadLog` | Record encoding, CRC verification, replay, torn-tail truncation |
| `store/KeyValueStore` | Durability ordering, concurrency control, counters |
| `net/ProtocolReader` | Line and length-prefixed framing |
| `net/KVClient` | Client library used by the tools |
| `server/KVServer` | Listen socket, worker pool, lifecycle |
| `server/ConnectionHandler` | Command dispatch, one connection at a time |
| `tools/Bench` | Closed-loop load generator with latency percentiles |
| `tools/Workload` | Deterministic write/verify used by the crash tests |

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

---

## Running it

```bash
./scripts/build.sh
java -cp build kvstore.Main --port 7379 --data data/kvstore.wal --sync every
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--port` | `7379` | Listen port |
| `--threads` | `32` | Worker pool size |
| `--data` | `data/kvstore.wal` | Log file path |
| `--sync` | `every` | `every` fsyncs each write; `never` leaves it to the OS |

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

Stage 2 turns this into a replicated store:

- [ ] Static 3-node cluster membership and a peer RPC channel
- [ ] Raft leader election (terms, `RequestVote`, randomised election timeouts)
- [ ] Log replication with `AppendEntries` and a commit index
- [ ] Reads served only by the leader, writes acknowledged after a quorum
- [ ] Chaos test: kill the leader mid-write, assert a new leader is elected and
      no acknowledged write is lost
- [ ] Log compaction via snapshotting, so replay time stops growing forever

Known limitations of stage 1, all addressed by the items above or listed honestly
as out of scope:

- The log grows without bound; there is no compaction yet.
- The whole dataset must fit in memory.
- No replication, so the node is a single point of failure.
- No authentication or TLS; bind to localhost.
- One lock for all writes, so write throughput will not scale past a few cores.
