# QWP transport support — design (DRAFT)

Status: **draft** — the original "agreed" version was revised after a
source-level review (2026-08-14) found the commit/DLQ/replay model unsound.
Needs re-agreement before implementation.
Targets: `questdb-client` 1.3.8 (already pinned; released 1.3.8 contains the
full QWP client including NACK-policy v2), QuestDB server v10+ (first release
with QWP) for tests.
Review baseline: connector `ef15b0a`, client `2489b243`, questdb `cedd6dab60`,
kafka `696d3ea37e`.

## Why QWP is not "a third schema string"

The connector's HTTP path is built on two assumptions QWP breaks:

1. **`flush()` returning means the data is durable.** Over QWP `flush()` is an
   asynchronous publish; durability is confirmed only by server acks tracked as
   frame sequence numbers (`flushAndGetSequence()`, `getAckedFsn()`,
   `awaitAckedFsn()`, `drain()`).
2. **Errors surface synchronously as exceptions from `flush()`.** Over QWP
   errors arrive asynchronously as structured `SenderError`s with a policy:
   `RETRIABLE` is retried by the client (reconnect + replay from the acked
   watermark) but can **poison-escalate** to a terminal
   `PROTOCOL_VIOLATION` after `max_frame_rejections` consecutive strikes on
   the same frame; only `RETRIABLE_OTHER` (`NOT_WRITABLE`) is exempt from
   striking and retries indefinitely; `TERMINAL` latches and throws on the
   next producer call.

Today a `ws::` conf string silently falls into the TCP branch of
`ClientConfUtils.patchConfStr` → per-batch synchronous flush, `max.retries`
retry loop, no DLQ, and — worst — `preCommit` committing offsets after a
`flush()` that guarantees nothing.

## Delivery contract: at-least-once, throughout

At-least-once is the contract **including within one live sender**, not just
across restarts/rebalances:

- The server commits a multi-table batch's tables sequentially and aborts on
  the first non-dropped-table failure — earlier tables may already be
  committed when the NACK arrives (`QwpTudCache`). "NACK ⇒ batch atomically
  not applied" holds per table, not per batch.
- A flush split across multiple frames can publish a prefix before failing;
  replay then duplicates that prefix (documented in `QwpWebSocketSender`).

Server-side `DEDUP UPSERT KEYS` is the standing mitigation and the docs must
recommend it for QWP generally, not only for the restart case.

## Decisions

| Question | Decision |
|---|---|
| Offset commits vs acks | Non-blocking FSN-gated ledger commits in `preCommit`; close/stop drain is **best-effort server cleanup only** (see below — it cannot affect the final commit) |
| Client store-and-forward | Memory-only: Kafka is the durable log; `sf_dir`/`sf_durability` rejected with `ConfigException` |
| TERMINAL errors | **Category matrix** (see below): only `SCHEMA_MISMATCH` is DLQ-eligible by default; everything else fails the task. Commit invariant: **never the rejected record or any later incomplete record** — a previously ACKed prefix may already have committed (see Commit path) |
| Client retries forever | `progress.timeout.ms` (default 300 000): task fails from **`put()`** (including empty `put()`) if data is pending and `ackedFsn` has not advanced for that long |
| Versions | Client 1.3.8 as-is; compatibility tests pinned to exact versions: Kafka 3.6.0+ and `questdb/questdb:10.0.0` (not a floating "v10") |
| Kafka baseline | **Compile against Kafka ≥ 3.6** (KIP-793 original-coordinate APIs; current pom pins 3.3.1). QWP requires a 3.6+ Connect runtime; the dispatcher rejects QWP on older runtimes fast, and only the QWP delegate calls the new APIs, so legacy HTTP/TCP configs keep working on older workers |
| Rollout | Experimental first: dedicated QWP test class; docs mark `ws::` experimental; 3-way test-matrix parameterization is a follow-up |

## Architecture

### One stable task class, dispatching on the task worker

QWP does not become a third branch inside the legacy task logic (whose single
`httpTransport` boolean already gates six behaviors) — but transport
selection **cannot** go through `SinkConnector.taskClass()` either: that
method runs on the worker hosting the connector instance, Kafka stamps the
returned class name into the task configurations, and tasks may execute on
**other** workers whose local environment differs. With worker-local
`QDB_CLIENT_CONF`, an HTTP-configured connector worker plus a task worker
carrying `QDB_CLIENT_CONF=ws::…` would run the legacy task on a QWP config —
exactly the unsafe path this design exists to eliminate. Instead:

- `taskClass()` always returns the one stable `QuestDBSinkTask.class` (name
  kept for config compatibility), which becomes a thin **dispatcher**: its
  `start(props)` resolves the conf string **on the task worker** — same
  precedence (`client.conf.string`, then `QDB_CLIENT_CONF`) and
  `ConfStringEnvInterpolator` expansion as sender creation — then
  instantiates and delegates every `SinkTask` callback (`put`, `flush`,
  `preCommit`, `open`, `close`, `stop`) to one of two implementations:
  the legacy HTTP/TCP delegate (today's logic, extracted) or the QWP
  delegate. The Kafka ≥ 3.6 runtime guard runs in the dispatcher when QWP
  is selected.
- The transport-independent record→row mapping (`handleSingleRecord`/
  `handleObject`, timestamp handling, `symbols`/`doubles`, BufferingSender
  wrapping) — ~80% of today's task — is extracted into a shared
  `RecordToRowHandler` collaborator composed by both tasks. Each task owns
  only its lifecycle: flush cadence, offset commits, error handling.
- The extraction lands as a pure refactor commit first; the legacy task stays
  behavior-compatible for HTTP/TCP, then the QWP delegate lands on top.

### Config (`ClientConfUtils`, `QuestDBSinkConnectorConfig`)

- `ws::`/`wss::` schema → QWP task.
- Strip `auto_flush_rows`/`auto_flush_interval` into `FlushConfig`. The
  original plan appended `auto_flush=off;`, but client 1.3.8 rejects that for
  WebSocket (`Sender.java:4436`); the implementation appends
  `auto_flush_rows=off;auto_flush_bytes=off;auto_flush_interval=2147483646;`
  instead (largest accepted finite timer, ~24.8 days — effectively never
  fires, and any client-internal flush would carry FSNs below the
  connector's next `flushAndGetSequence()`, so ack coverage stays
  cumulative). See the implementation diary.
- **Reject `auto_flush_bytes`** for QWP: the connector has no reliable byte
  accounting, so silently accepting a byte trigger it cannot honor would lie.
  (Byte-triggered flushing is a follow-up if byte accounting is added.)
- Reject `sf_dir` and `sf_durability` with `ConfigException` (memory-only
  SF). `sf_max_total_bytes` and `sf_append_deadline_millis` are **kept and
  validated** — they bound the in-RAM window and the block-then-throw
  deadline (see Backpressure).
- New connector keys (QWP only):
  - `progress.timeout.ms` — LONG, default `300000`.
  - `max.inflight.rows` — INT, default `150000`. **A soft trigger, not a hard
    cap**: the batch already handed to `put()` is always accepted, so the
    retained window can overshoot by up to one poll batch, and row count does
    not bound retained-object bytes. A byte budget is a possible follow-up.
- `max.retries`/`retry.backoff.ms` remain TCP-only; the QWP client owns
  transient retries.
- Sender construction moves from `Sender.fromConfig(str)` to the builder path
  so the connector can attach `SenderErrorHandler` (and optionally
  `SenderConnectionListener` for INFO logging).

### Offset ledger

An FSN-only high-watermark is insufficient: tombstones produce no row,
client-invalid records go straight to the DLQ, and a flush that publishes
nothing returns `-1` — none of which may strand committable offsets. But a
ledger of "every input record" is equally wrong: converter/SMT-filtered
records (an SMT returning `null`, tolerated converter errors) are consumed by
Connect yet **never delivered to `put()`**, so such a ledger would have
permanent holes and stall commits forever.

The ledger therefore tracks only **delivered records that are not yet
complete**, each with:

- its original Kafka coordinates — `originalTopic()`,
  `originalKafkaPartition()`, `originalKafkaOffset()` (pre-SMT). These are
  KIP-793 APIs, first released in Kafka 3.6.0 — hence the Kafka ≥ 3.6 compile
  baseline and the dispatcher's runtime guard. Transformed
  coordinates are **not** an acceptable fallback: topic/partition-changing
  SMTs would make commits incorrect;
- a completion dependency: the FSN of the flush entry that covers it, the
  completed `ErrantRecordReporter` future for a DLQ'd record, or
  immediately-complete for no-op records (tombstones — which simply never
  enter the incomplete set).

Committability is a **clamp, not a frontier**: Connect's own
`currentOffsets` already accounts for filtered records; the task only holds
it back while delivered work is in flight.

Flush entries record `(fsn, ledger range)` at each `flushAndGetSequence()`
call, made at the existing connector-driven cadence (rows ≥ `autoFlushRows`,
interval elapsed, `allowed.lag` pacing via `context.timeout()`).

### Commit path

- `preCommit(currentOffsets)`: read `getAckedFsn()`, mark covered flush
  entries (and completed DLQ futures) complete and drop them from the
  incomplete set; then return, per partition,
  `min(currentOffsets, earliest incomplete original offset)` — and plain
  `currentOffsets` for partitions with nothing incomplete. Never blocks and
  **never throws for stall detection** — Connect catches `preCommit`
  exceptions and rewinds offsets rather than failing the task. `preCommit`
  does, however, run the zero-time terminal probe itself, **catching the
  throw internally**: on a latched terminal it records the failure for the
  next `put()` to raise and returns an empty map, minimizing (but not
  eliminating — see the terminal-matrix invariant) the window in which an
  ACKed prefix commits after a terminal.
- Every `put()` — including empty calls — **starts with a nonblocking
  terminal-error probe**: `sender.awaitAckedFsn(sender.getAckedFsn(), 0)`.
  `getAckedFsn()` is a pure snapshot that does **not** check the sender's
  latched error, while `awaitAckedFsn()` does; without the probe, an idle or
  paused task would misreport a terminal NACK as generic lack of progress
  until `progress.timeout.ms` expires. The probe throws the typed latched
  exception immediately, routing into the terminal-category handling below.
- Stall detection follows the probe, still **before the empty-collection
  early return**: if records are pending and `getAckedFsn()` has not advanced
  for `progress.timeout.ms`, throw `ConnectException`. An all-paused task
  still receives empty `put()` calls, so both checks fire even under
  backpressure.
- `close(partitions)`/`stop()`: bounded `drain(timeout)` as **best-effort
  server cleanup only**. Connect's callback order makes acks arriving during
  `close()` unable to affect the commit: `preCommit` runs first, `close`
  runs in a `finally`, and the commit uses `preCommit`'s return value;
  `stop()` runs after the final commit entirely. Clean-rebalance redelivery
  of the unacked tail is accepted (and deduped server-side); the design must
  not promise otherwise.
- **Partial cooperative rebalances**: `close(partitions)` may revoke only a
  subset of the assignment while the sender, flush entries, and retained
  window are global and mix partitions. The task therefore:
  - tracks the current assignment via `open(partitions)`/`close(partitions)`
    (neither is overridden today — the dispatcher must forward both to the QWP delegate);
  - does **not** close the global sender on a partial revoke;
  - after the bounded `drain` attempt, removes the revoked partitions'
    records from the ledger and retained window — their uncommitted offsets
    redeliver at the new owner;
  - retains only still-assigned records for any later sender reconstruction
    (isolation replay included);
  - applies backpressure pauses to **all currently assigned partitions**,
    whatever the assignment is at that moment.

### Backpressure

Two layers, and the second is not optional:

1. **Connector pause (first line, nonblocking)**: when the retained window
   exceeds `max.inflight.rows` (soft trigger, above): `context.pause(...)` on
   **all currently assigned partitions** + `context.timeout(...)`; resume
   once acks release enough.
2. **Client byte cap (hard backstop)**: memory-mode SF still enforces
   `sf_max_total_bytes`; once reached, the sender **blocks inside the
   producer call for up to `sf_append_deadline_millis`, then throws**. The
   pause layer cannot prevent this (one poll batch can overshoot, and rows
   don't bound bytes), so the connector must own both knobs:
   - expose and validate `sf_max_total_bytes` and
     `sf_append_deadline_millis` for QWP (never strip them);
   - default `sf_append_deadline_millis` to a conservative 30 000. The task
     **cannot** validate against the effective poll interval —
     `SinkTaskContext` does not expose consumer configuration, which Connect
     assembles separately from worker-global `consumer.*` settings plus
     connector `consumer.override.*` overrides. So: validate the deadline
     only against an explicit `consumer.override.max.poll.interval.ms`
     **when present in the connector props**, and otherwise document the
     operator invariant (deadline safely below the worker's
     `max.poll.interval.ms`, default 300 000 — a `put()` blocked past it
     evicts the consumer). The connector must not claim universal
     enforcement;
   - classify the deadline-exceeded throw as **task-fatal capacity
     exhaustion** — not record-attributable, never DLQ-eligible.

`max.inflight.rows` alone does not bound retained `SinkRecord` heap usage;
the byte cap is the honest limit and the docs must say so.

### Error handling

- The existing `catch (LineSenderException | HttpClientException)` **already
  covers** the QWP exception types: `LineSenderServerException extends
  LineSenderException`, and `QwpDurableAckMismatchException`/
  `QwpIngressRoleRejectedException` extend `HttpClientException`. No catch
  changes needed — the work is in classifying, not catching.
- `SenderErrorHandler` runs on a dedicated daemon dispatcher thread (not the
  I/O thread) with a **bounded inbox that drops surplus notifications** — so
  the handler is observability only (WARN-log RETRIABLE/RETRIABLE_OTHER,
  record last-seen TERMINAL detail). The **latched exception thrown by the
  next producer call is the source of truth** for terminal state.
- **Terminal category matrix** (default behavior):

  | Category | Action |
  |---|---|
  | `SCHEMA_MISMATCH` | DLQ-eligible: record-attributable, deterministic under replay → replay isolation (below) |
  | `PARSE_ERROR` | Fail the task by default — it means malformed QWP payload, most likely an encoder/client defect, not a bad Kafka record; isolation only with explicit opt-in |
  | `SECURITY_ERROR` | Fail the task, commit nothing — usually an ACL/config problem, not a bad record |
  | `PROTOCOL_VIOLATION` (poison escalation) | Fail the task, commit nothing — began life as retriable `WRITE_ERROR`/`INTERNAL_ERROR`; blaming records would DLQ good data on an unhealthy server |
  | Durable-ack mismatch, transport/config failures | Fail the task, commit nothing — not record-attributable |
  | SF capacity exhaustion (`sf_append_deadline_millis` exceeded) | Fail the task, commit nothing — backpressure/capacity, not record-attributable |

  Any broader DLQ eligibility (`PARSE_ERROR`, `PROTOCOL_VIOLATION`, …) must
  be an explicit opt-in config, never the default.

  "Fail the task, commit nothing" is **not a realizable contract**: Connect
  checks whether a commit is due before polling/calling `put()`, so an async
  terminal can latch and `preCommit` can run — legitimately committing an
  ACKed prefix or unrelated partitions — before the producer-thread probe
  observes it, and `sender = null` cannot undo that commit. The invariant is
  therefore: **the rejected record and every later incomplete record are
  never committed; previously ACKed work may commit.** That is exactly what
  the ledger clamp guarantees (acks never advance past a rejected frame).
- **Replay isolation (DLQ-eligible terminals only) — a resumable state
  machine, not a synchronous loop.** Scope first: the connector cannot narrow
  a rejection to records within a flush — `flushAndGetSequence()` returns
  only the highest FSN and one flush can split across multiple internal
  frames — so the record-by-record probe targets **every record of the flush
  entry whose FSN range overlaps the rejected FSN** (exact frame-level
  narrowing needs a new client API — follow-up). Mechanics:
  - Recreating the sender discards **all** client-side state, so the recovery
    set is the entire retained window: entries before the overlapping one,
    the overlapping entry, entries after it, **and the unflushed tail** —
    non-suspect portions replay as whole batches, only the overlapping entry
    record-by-record.
  - A record is DLQ'd **only when its probe throws a typed terminal**.
    `drain(timeout) == false` means timeout, never rejection — timeouts do
    not DLQ; they count toward `progress.timeout.ms` like any stall.
  - Every failed probe latches the sender permanently → **recreate the
    sender after each failure** before probing the next record.
  - Isolation runs as a state machine processed in **bounded slices across
    successive `put()` calls** (empty ones included), with all assigned
    partitions paused for its duration — a large window isolated
    synchronously would blow `max.poll.interval.ms` and trigger a rebalance.
  - **The current non-empty batch must not be swallowed by a slice.** Kafka
    treats any normal return from `put()` as successful delivery: it advances
    `currentOffsets` and clears the batch, and an un-admitted batch has no
    ledger entry to clamp its offsets. Two cases:
    - recovery encountered **before** the incoming collection is admitted to
      the ledger (e.g. the top-of-`put()` probe throws): process one bounded
      slice, then throw `RetriableException` — Kafka preserves and
      redelivers that exact batch;
    - the collection was **already ledger-admitted** when the terminal
      surfaced (e.g. mid-`put()` during a flush): returning normally after
      pausing is safe — the records are retained and the clamp holds their
      offsets.
  - If isolation itself stops making progress for `progress.timeout.ms`,
    fail the task. On completion: DLQ'd records complete via their reporter
    futures, partitions resume.
- `dlq.send.batch.on.error=true`: entire retained window to the DLQ — again
  only for DLQ-eligible categories.
- No DLQ configured: any terminal → `ConnectException`, task fails.

### BufferingSender fix (required)

`BufferingSender` (used when `symbols` is set) does not override the `Sender`
interface's default `flushAndGetSequence()` / `awaitAckedFsn()` / `drain()` /
`getAckedFsn()`, so they silently degrade to no-op "success" — an ack-gated
commit would be a lie. Override all four to delegate.

## Test plan (experimental-first)

New `QwpSinkConnectorTest` (embedded, against `questdb/questdb` v10) pinning
the new semantics. Required acceptance tests from the review:

- `SECURITY_ERROR`/`PROTOCOL_VIOLATION`/`PARSE_ERROR` terminals fail the task
  with **no** DLQ output and **no commit of the rejected record or anything
  after it**; an already-ACKed prefix committing is permitted, so the test
  asserts the invariant, not zero advancement (parse-error DLQ only under
  the explicit opt-in).
- Isolation of a large retained window proceeds in slices across multiple
  `put()` calls with partitions paused — no `max.poll.interval.ms` rebalance;
  exercised at a genuinely large workload.
- Isolation corner cases: multiple bad records in one entry; good records in
  entries **after** the rejected one; an unflushed tail present when the
  terminal hits; isolation that stops making progress hits
  `progress.timeout.ms` and fails the task; isolation overlapping a partial
  revocation.
- A `drain` timeout during a record probe does **not** DLQ the record.
- Task selection honors `QDB_CLIENT_CONF` and env interpolation: an
  environment-provided `ws::` config on the **task worker** selects the QWP
  delegate — including when the connector-hosting worker lacks that env var
  (the taskClass()-selection failure mode).
- SF byte-cap exhaustion (append deadline exceeded) fails the task with no
  DLQ output and no commit beyond ACKed work.
- Rebalance callback order: acks arriving during `close()` are intentionally
  not committed (documented-duplicate test).
- Partial cooperative rebalance where one flush entry spans retained and
  revoked partitions: sender stays open, revoked records leave the ledger,
  still-assigned records stay committable — including a terminal arriving
  **after** the revocation, whose isolation replays only still-assigned
  records.
- An all-paused stalled task fails from `put(empty)` via
  `progress.timeout.ms`.
- A terminal NACK on an otherwise idle/paused task surfaces as the typed
  terminal error on the next `put(empty)` via the nonblocking probe — not as
  a stall timeout.
- Previous sender terminal already latched + next `put()` is **non-empty**:
  the batch is redelivered via `RetriableException`, never acknowledged by a
  normal return, and no record of it is lost or committed.
- A flush split across multiple FSNs uses conservative whole-entry isolation.
- Tombstone-only and client-DLQ-only batches advance original per-partition
  offsets (ledger completeness).
- An SMT returning `null` (filtered records) and converter errors tolerated
  by Connect: commits still advance past the filtered offsets (the clamp
  model — no ledger holes).
- Multi-table partial-commit duplication observed and deduped under the
  documented at-least-once contract.
- A topic/partition-renaming SMT in front of the QWP delegate — commits must use
  original coordinates (pins the KIP-793 dependency). The pre-3.6 runtime
  guard itself is covered by a unit test of the version check, not a
  container matrix.

Plus: watermark/ledger commits, replay-isolate DLQ ordering, batch-DLQ mode,
pause/resume backpressure, outage recovery, `symbols`/BufferingSender
delegation, `ClientConfUtilsTest` coverage (`ws::` mapping, `auto_flush_bytes`
rejection, `sf_dir` rejection). Docs: `ws::` experimental; recommend
`DEDUP UPSERT KEYS` for all QWP deployments.

Follow-ups (out of scope): 3-way parameterization of the embedded matrix,
chaos IT over QWP, frame-level DLQ narrowing (needs client API), byte-budget
backpressure, byte-triggered flushing.
