# QWP transport implementation diary

This diary records implementation decisions that differ from
`design/qwp-transport.md`. Entries are append-only while the implementation is
in progress. If the implementation follows the design, no deviation is
recorded.

## 2026-08-14

- No design deviations yet.
- Baseline: commit `ef15b0a`; `design/` was already untracked before implementation.
- The local correctness baseline ran 669 tests with no assertion failures and
  four pre-existing errors: two Docker-environment failures and two JDK 25
  locale/parser failures. JaCoCo 0.8.12 also cannot instrument JDK 25 classes.
- Performance measurements will not run locally. Any performance study will
  use the quiet `domapc` host and a fixed-work workload; until then, changes
  receive correctness/lifecycle validation only and make no throughput claim.

### Open design values

- The draft does not name or default the best-effort drain timeout, the replay
  isolation slice budget, or the broader terminal-category opt-in. Proposed
  values were sent to the user before implementing those behaviors.

### Clarifications implemented provisionally

- Added `qwp.drain.timeout.ms` with a 30-second default. A rebalance callback
  needs a bound independent of the five-minute progress timeout.
- Added `qwp.isolation.slice.ms` with a 100-millisecond default. A time budget
  bounds producer-thread occupancy across payload sizes more honestly than a
  record-count budget.
- Added `qwp.dlq.terminal.categories`, defaulting to `SCHEMA_MISMATCH`. This
  gives the draft's explicit opt-in category policy a concrete configuration
  surface without broadening the safe default.

### Design deviations

- The draft requires appending `auto_flush=off` for QWP. QuestDB client 1.3.8
  rejects that configuration with `disabling auto-flush is not supported for
  WebSocket protocol`; the first remote smoke test proved the task could not
  start. The implementation instead appends `auto_flush_rows=off`,
  `auto_flush_bytes=off`, and `auto_flush_interval=2147483646` (the largest
  finite value accepted by the client). This preserves connector-driven
  publishing in normal operation, while satisfying the client's requirement
  that a finite safety timer exist.

### Reflection and validation

- A deterministic 1,000-record isolation test exposed an early-resume bug:
  the normal low-watermark path resumed partitions after the first recovered
  record. Recovery now suppresses that path and resumes only after the state
  machine finishes (or remains paused if the inflight threshold still applies).
- The final focused suite passed 34 tests on `domapc`, including an embedded
  Kafka 3.6 worker and exact `questdb/questdb:10.0.0` QWP ingestion smoke. The
  isolated checkout is `/home/jara/devel/oss/kafka-qwp-codex.Po8gM8`; no test
  data or build workspace was placed under `/tmp`.
- The local full suite ran 696 tests with zero assertion failures and the same
  four environment/JDK errors recorded at baseline (two unavailable-Docker
  errors and two JDK 25 locale/parser errors), plus one skipped test.
- No throughput or latency benchmark was run, so this implementation makes no
  performance-improvement claim. The hot path uses linear ledger scans, avoids
  per-field bookkeeping, and avoids a redundant per-poll reference array, but
  those are source-level properties rather than measured results.

### Acceptance-test limitations

- The exact-version integration test covers successful QWP ingestion. Terminal
  categories, replay slicing, multiple rejected records, unflushed tails,
  partial revocation, capacity failure, and stalled empty puts are covered with
  deterministic sender/context fakes because the embedded server harness has
  no deterministic fault-injection API for those conditions.
- The multi-worker `QDB_CLIENT_CONF` topology, observed multi-table partial
  commit duplication with server-side deduplication, and a fixed-work
  performance campaign remain follow-up validation before removing the
  experimental label. This is a test-plan deviation, not a weaker runtime
  delivery contract.

## 2026-08-14 — review findings

### Plain row-building exceptions and the DLQ

- Audited the Java client's release tag `1.3.8` (release commit `9e9b2e9b`;
  this repository names the tag `1.3.8`, not `v1.3.8`). A plain
  `LineSenderException` from a QWP row call is not reliably attributable to
  that record:
  - `QwpWebSocketSender` row methods call `checkNotClosed()` before local
    validation (`longColumn`, lines 2293-2303; `stringColumn`/`symbol`, lines
    2710-2737; `table`, lines 2742-2754).
  - `checkNotClosed()` delegates to `checkConnectionError()` (lines
    3418-3443), which throws the plain `LineSenderException` connection latch
    or calls `CursorWebSocketSendLoop.checkError()`. The latter latches a
    general `LineSenderException` and wraps non-client failures into one
    (`CursorWebSocketSendLoop`, lines 458-466, 1171-1176, and 2447-2451).
  - `at()`/`atNow()` call `sendRow()` (lines 882-920 and 4935-4970), which can
    enter initial connection and auto-flush paths. Buffer-recycle timeout and
    cursor append failures are plain `LineSenderException`s
    (`QwpWebSocketSender`, lines 4867-4927). The cursor SF append deadline also
    throws a plain `LineSenderException` on capacity exhaustion
    (`CursorSendEngine`, lines 918-935).
  - Structured server rejections are distinguishable because they are latched
    as `LineSenderServerException` (for example `CursorWebSocketSendLoop`,
    lines 1791-1804).
- Decision: keep `wrapSenderErrors=false` for QWP. Wrapping every other
  `LineSenderException` into `InvalidDataException` would make connection or
  SF-capacity failures record-DLQ eligible, violating the safety invariant.
  The README now states that plain client validation errors therefore fail the
  task as a conservative limitation. A regression test pins that a typed
  terminal raised during row construction reaches terminal handling and never
  the DLQ.

### Flush and commit scheduling

- Removed `QwpSinkTask.flush(Map)`. The QWP `preCommit()` implementation does
  not delegate to the default `SinkTask.preCommit()`/`flush()` path, and all
  publishing remains owned by `put()` cadence and recovery. The only unit test
  that called `flush()` directly now publishes by reaching `auto_flush_rows`.
- Moved `context.requestCommit()` out of `publishPendingRows()`. Publishing is
  not progress because offsets are still ACK-clamped. `updateCompletions()` now
  requests a commit only after it removes at least one retained record due to
  an ACK or a completed DLQ future; repeated polls without new completion do
  not request another commit.

### Deferred corner

- Review finding 4 remains intentionally deferred: a typed terminal whose FSN
  span has no surviving overlapping flush entry still fails the task. This
  change does not attempt to reconstruct or recover that range.

### Review-finding validation

- The focused local suite
  `mvn -pl connector test -Djacoco.skip=true
  -Dtest=QwpSinkTaskTest,QwpSinkConnectorTest,QuestDBSinkTaskTest,ClientConfUtilsTest,QuestDBSinkConnectorConfigTest,BufferingSenderTest`
  passed 35 tests with zero failures or errors and one environment skip.
  `QwpSinkTaskTest` contributed 20 passing tests.
- Running `QwpSinkConnectorTest` alone found zero runnable tests because the
  class is guarded by `@Testcontainers(disabledWithoutDocker = true)` and this
  host has no usable Docker environment. The earlier `domapc` exact-version
  integration result remains recorded above; this review pass did not repeat
  it because no server-facing transport behavior changed.
- `git diff --check` passed. No performance measurement was run and no
  performance claim is made for these correctness changes.
- After syncing only the four review-touched files to the existing isolated
  `domapc` checkout, the same focused suite passed all 35 tests with zero
  failures, errors, or skips. This included the Docker-backed
  `QwpSinkConnectorTest` against QuestDB 10.0.0. This was correctness
  validation, not a performance measurement.
- The preceding local-only note was accurate when written but is superseded
  by this later remote run; it is retained to preserve the append-only diary.
