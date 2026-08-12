# Project handoff

Context carried over from a planning session. Read this first.

## Who and why

Final-year B.Tech CS student (Amrita Vishwa Vidyapeetham, 2023–2027), targeting
SDE roles, interviewing within months. This project exists to fill specific gaps
on his resume, which previously held two near-identical projects (Next.js +
FastAPI + PostgreSQL + Gemini API) and left Java, C++, DSA, systems, and
infrastructure claims unproven.

This repo is the systems-depth project: Java, no frameworks, no AI, no web layer.
Concurrency, TCP networking, disk I/O, crash safety. Keep it that way — adding an
AI feature or a web dashboard would defeat the entire point.

Timeline: stage 1 done, stage 2 (Raft) budgeted at 4–6 weeks of evenings. DSA
practice takes priority over this project if the two ever compete for time.

## Conventions

- **No AI attribution in commits.** `~/.claude/settings.json` should contain
  `{"attribution": {"commit": "", "pr": ""}}`. He does not want Claude appearing
  in the GitHub contributors list. There is also a `commit-msg` hook stripping
  `Co-Authored-By: Claude` and `Generated with [Claude Code]` lines.
- **No external dependencies.** Plain `javac`, no Maven or Gradle. This is
  deliberate. Do not introduce a build tool or a test framework without asking.
- **He must be able to explain every line in an interview.** When implementing,
  explain the *why* — especially ordering and concurrency decisions. Prefer
  teaching over speed. Do not silently refactor code he hasn't read yet.
- Comments should explain reasoning, not restate the code.

## What exists (stage 1, complete and tested)

A durable single-node key-value store. `./scripts/build.sh` compiles clean under
`-Xlint:all`. See README.md for the full architecture, record format, and protocol.

```
src/main/java/kvstore/
  Main.java                    arg parsing, recovery, startup, shutdown hook
  wal/WriteAheadLog.java       record encoding, CRC, replay, torn-tail truncation
  store/KeyValueStore.java     durability ordering, concurrency, counters
  net/ProtocolReader.java      line + length-prefixed framing
  net/KVClient.java            client library
  server/KVServer.java         listen socket, bounded worker pool
  server/ConnectionHandler.java command dispatch per connection
  tools/Bench.java             load generator, latency percentiles
  tools/Workload.java          deterministic write/verify for crash tests
scripts/{build,bench,crash_test}.sh
```

### Two invariants that must not be broken

1. **Log before apply.** `KeyValueStore.put` appends to the WAL *before* mutating
   the map. Reversing this lets a client read a value the log never recorded.
2. **Writes serialize on one lock.** So log order and map state agree. Reads take
   no lock (`ConcurrentHashMap`).

### Verified results

`./scripts/crash_test.sh` — 8/8 passing. Four scenarios: SIGKILL after 5,000
acknowledged writes (zero loss), SIGKILL after deletes (tombstones durable), 137
random bytes appended to the log (tail truncated), log truncated mid-record (node
starts, torn record dropped, writes still work). The kill is SIGKILL specifically
so no shutdown hook runs.

`./scripts/bench.sh` — measured on a 1-vCPU container, so conservative. 8 clients,
64B values: write+fsync 3,840 ops/sec (p50 1.66ms, p99 12.07ms); write without
fsync 15,460 ops/sec; read 25,815 ops/sec; mixed 17,604 ops/sec. **Re-run these
locally and update the README** — the 4× fsync gap is the headline result and the
best interview talking point in the project.

## Stage 2: Raft replication (next)

- [ ] Static 3-node membership, peer RPC channel
- [ ] Leader election: terms, `RequestVote`, randomised election timeouts
- [ ] Log replication: `AppendEntries`, commit index
- [ ] Leader-only reads, writes acknowledged after quorum
- [ ] Chaos test: kill the leader mid-write, assert new leader elected and no
      acknowledged write lost — **this test is the point of stage 2**
- [ ] Snapshotting for log compaction

If time runs short, a working partial Raft he can explain beats a complete one he
can't. Ship leader election + the chaos test and document what's simplified.

Known stage-1 limitations (don't treat as bugs): log grows unbounded, dataset must
fit in memory, single point of failure, no auth or TLS, one write lock caps write
scaling.

## Open tasks outside this repo

1. **Git init and first push.** Not yet a git repo. Set the attribution config
   before the first commit. Verify `git config user.email` matches a verified
   GitHub email.
2. **FinAI infra pass** (separate repo, ~2 weekends): pytest on the API layer,
   Dockerfile + compose (app/Postgres/Redis), GitHub Actions CI, Redis caching on
   read-heavy endpoints, fix N+1 queries, add indexes, Locust load test recording
   p50/p95 before and after, deploy with a live URL.
3. **FinAI history cleanup.** `claude` appears in that repo's contributors list.
   Needs `git filter-repo --message-callback` to strip `Co-Authored-By: Claude`
   and `Generated with [Claude Code]` trailers, then a force-push. Back up first;
   this rewrites every SHA.
4. **Resume rewrite** exists as a draft with `«...»` slots for real measurements.
   Every bullet needs a number or the adjective gets deleted. Do not list this
   project as "distributed" until stage 2 works.
