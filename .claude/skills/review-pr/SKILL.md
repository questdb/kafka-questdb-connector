---
name: review-pr
description: Review a GitHub pull request or local Git range against kafka-questdb-connector standards
argument-hint: "[PR number or URL | --range=<base>..<head>] [--level=0..3]"
allowed-tools: Bash, Read, Grep, Glob, Agent
---

# Review a kafka-questdb-connector pull request

**Usage:** `/review-pr [PR number or URL | --range=<base>..<head>] [--level=0..3]`

Review the PR or local range identified by the invocation arguments. When this skill is run
as `/skill:review-pr <args>`, the `<args>` are appended as a `User:` message; treat that text
as `$ARGUMENTS`. Parse exactly one review target: a PR number/URL, or `--range=<base>..<head>`.
The range head may be omitted (`--range=<base>..`) to review the working tree, including
uncommitted changes. If both targets are supplied, stop and ask which was intended. If neither
is supplied, ask for one.

**Tools this skill uses:** `Bash` for read-only `gh` and Git queries, `Read`, `Grep`, `Glob`,
and fresh-context agents through the Agent tool. Do not edit files or push.

## Review mindset

You are a senior engineer performing a blocking code review of a Kafka Connect sink connector.
This connector sits between Kafka and QuestDB in production pipelines: its defects show up as
lost rows, duplicated rows, silently stalled ingestion, or a consumer group that cannot
rebalance. Those are expensive to diagnose because the connector often keeps reporting itself
healthy while doing nothing. Be critical, thorough, and opinionated. Catch problems that would
hurt a user before they ship — not to be nice, and not to demonstrate thoroughness by volume.

**A review that blocks on everything blocks on nothing.** Every finding costs the author a CI
round-trip, and an inflated one costs the whole report its credibility. Reserve blocking
severity for defects with a real user consequence, report everything else honestly at the
severity it deserves, and approve when the gates pass. "Approve" is a normal, expected outcome
of reviewing competent work — not a failure of rigour.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand
  context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs live at
  callsites outside the diff that depend on contracts the diff quietly changed. In this repo
  that is usually the *other* transport: shared mapping code is reached by HTTP, TCP and QWP.
- **Discovery is not a finding.** Treat every concern — including one produced by several
  agents — as an untrusted hypothesis until it passes the Step 3b admission gate. Report every
  *admitted* issue at the severity its evidence earns; omit everything else. A review with zero
  findings is a successful outcome.
- **Falsify before you explain.** Search for the missing producer, unsupported configuration,
  omitted caller, retry, guard, and merge-base behaviour before building a narrative. Failure
  to disprove a hypothesis is not evidence for it, and uncertainty is never promoted to severity.
- **Prefer execution to reading.** This codebase is a distributed-systems component whose
  interesting states — a rebalance, a pending DLQ future, a client that flushed on its own
  cadence — are nearly impossible to establish by reading. A claim about ordering, retries,
  restarts, offsets, or acknowledgement timing needs an executed artifact. Static reading alone
  cannot admit one.
- **Keep the blast radius of the PR small.** This PR should fix what it set out to fix, plus
  anything this change demonstrably breaks. Pre-existing bugs and residual hardening whose
  behaviour is unchanged from base are never findings against this PR and never affect its
  verdict. The one exception is a pre-existing bug this PR demonstrably moves onto a live path.
  A fully proved pre-existing bug leaves as a Step 4 adjacent issue draft rather than being
  thrown away.
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach".
- **Think adversarially.** For each change ask: what happens on a rebalance? On a task restart?
  When the server is reachable but not acknowledging? When the batch is 500 records instead of
  3? When the DLQ producer has not acked yet? When two workers each run a task?
- **Check what's missing**, not just what's there. Missing tests, missing error handling,
  missing transport coverage, missing documentation for non-obvious behaviour.
- **Untested changed behaviour is a coverage risk, not proof of a defect.** Missing tests alone
  cannot make a finding Critical. A Critical coverage gap must identify a supported, reachable
  population and a credible regression mode with material impact.
- **Verify every claim.** Treat the PR description as an unverified hypothesis. If it says
  "fix", verify the bug existed and the fix is correct. If it claims a throughput win, look for
  the measurement.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`.
**If no level is given, default to 0.** Strip the level token and any `--range=` token before
feeding the remainder to `gh`.

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 2.4, 2.6, 4. Skip Step 2.5 and agent fanout. Review the diff inline for correctness, Connect-contract violations, transport parity, tests. Every candidate still passes the Step 3b admission gate inline from a blank evidence form. |
| **1** | Adds Step 2.5a and 2.5e when test code is present. Run Agent 1 plus at most **two** applicable roles. Run an independent falsification task per surviving atomic candidate. |
| **2** | Full Step 2.5, with 2.5b restricted to `public`/`protected` symbols. Agent 1 plus at most **four** change-relevant roles. Independent falsification per candidate. |
| **3** | Full Step 2.5 and the complete admission protocol. At most **six** applicable roles: Agent 1 always; Agent 9 when changed symbols have out-of-diff callers; Agents 2-6 only when their domain is touched; Agents 12-14 only for changed tests or a fix claim; Agent 10 only when a distinct adversarial pass is warranted. Depth comes from producer/reachability evidence and independent falsification, not agent count. |

State the chosen level in one line at the start of the review (e.g. "Reviewing PR #42 at
level 2"). If the level was defaulted, mention that level 3 exists.

## Spawning review agents

Steps 3 and 3b use fresh-context agents through the Agent tool, one task per role or atomic
falsification candidate. Each task is self-contained and read-only. Discovery tasks receive the
diff, the Step 2.4 provenance verdicts, the Step 2.5 surface map, the Step 2.6 coverage map,
role instructions, and the candidate contract. Agent 10 is a deliberate reduced-context
exception. Step 3b falsifiers receive only the neutral proposition, revision identities,
relevant files, and raw artifact paths.

Use a shared temporary artifact for large maps rather than pasting them repeatedly. Never pass
the discovery narrative, proposed severity/fix, votes, or verification claims to a falsifier.
The parent owns role selection, the private ledger, admission, severity, and output; children
return candidates or falsification evidence only.

## Step 1: Gather PR context

Every mode must end this step with **`$BASE`** set — the commit the change is measured against.
`$BASE` is required by every behavioural finding's same-trigger base check; a review that never
established it cannot attribute anything.

### GitHub PR

```bash
PR='<PR number or URL from $ARGUMENTS, with any level token removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr view "$PR" --comments
BASE=$(gh pr view "$PR" --json baseRefOid --jq .baseRefOid)
```

### Local range (`--range`)

```bash
BASE='<base from --range>'
HEAD='<head from --range, or empty for the working tree>'
git diff "$BASE"${HEAD:+"...$HEAD"} --stat
git diff "$BASE"${HEAD:+"...$HEAD"}
```

With `<head>` empty the diff includes uncommitted working-tree changes. Untracked files do not
appear in `git diff` — list them with `git status --porcelain` and read any that are part of
the change, especially new test files, or the Step 2.6 map will silently miss them.

In range mode: **skip Step 2 entirely** and say so. Every other step runs unchanged.

## Step 2: PR title and description

**Skipped in `--range` mode.**

- Title follows Conventional Commits: `type(scope): description` (this repo uses them)
- Description explains user impact, not just mechanism
- If fixing an issue, `Fixes #NNN` is at the top of the body
- Tone is level-headed, no superlatives or bold emphasis on numbers
- Bundled related fixes are allowed; do not demand a split

## Step 2.4: Dependency provenance (mandatory at every level)

This repository has **no git submodules**; state that once and move on. Its equivalent scope
question is a **dependency version bump**, which carries somebody else's behaviour changes into
this PR. Before attributing any behaviour to the diff, classify every bump in `pom.xml`:

```bash
git diff "$BASE...HEAD" -- pom.xml '**/pom.xml' | grep -E '^[+-].*<(version|.*\.version)>'
```

Classify each as exactly one of:

- **UPSTREAM-RELEASE** — a released artifact version bump (`questdb-client`, `kafka-clients`,
  `connect-api`). The behaviour inside it is not this PR's work and not this PR's
  responsibility. The only legitimate finding is an **integration** defect: code in this diff
  uses the newly-bumped dependency incorrectly, or relies on behaviour the new version changed.
  That finding lives at the callsite in this diff.
- **IN-SCOPE** — a snapshot, a local build, or a bump the PR itself authored upstream. Its
  behaviour changes are part of this logical change.
- **UNRESOLVED** — cannot be determined. Say so, treat as IN-SCOPE for safety, and state that
  the scope decision was made without provenance.

`questdb-client` bumps matter more here than anywhere else: the client owns the sender contract
(auto-flush cadence, frame sequence numbers, column-type memory, `cancelRow()` legality per
transport, close/flush timeouts). A bump can change connector behaviour with no connector diff
at all. When one is present, read the client's changelog or diff for those contracts and record
what moved.

## Step 2.5: Map the change surface

Mandatory at level 2+. Use Grep and Glob — do not reason about callsites from memory. The output
is required input for every Step 3 agent.

### 2.5a Semantic delta per changed symbol

For every modified or added method, field, config key, or constant, write:

- **Symbol:** fully-qualified name
- **Before / After:** signature, thrown exceptions, mutation, ordering/idempotency guarantees,
  allocation behaviour, thread-affinity, which transports reach it
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "simplified" are not acceptable deltas. If nothing changed, write
"no behavioural change" — but only after checking.

### 2.5b Callsite inventory

For every changed symbol that is `public`, `protected`, or package-private, `rg` the whole
repository for callsites, overrides, and references outside the diff. Also search for:

- subclasses that override the method, and interfaces that declare it
- reflective callers (`Class.forName`, `getMethod`) — the task dispatcher loads `QwpSinkTask`
  reflectively, so a rename will not fail compilation
- `META-INF/services` entries and `ServiceLoader` registrations (KIP-898 plugin discovery)
- config-key strings, which are referenced from tests, the readme, and sample configs

A changed non-private symbol with zero recorded `rg` calls in the trace is a skill violation.

### 2.5c Implicit contract list

For each changed symbol, one line per item, before vs after:

- Throws on which inputs, and which exception type — the type decides the outcome here:
  `RetriableException` means redelivery, `InvalidDataException` means DLQ-eligible on some
  transports, anything else fails the task
- Which thread runs it (Connect task thread vs a client callback thread)
- Whether it may block, and for how long
- Whether it mutates offset/ledger state, and whether that mutation is idempotent under replay
- Which transports reach it (`http`, `https`, `tcp`, `tcps`, `ws`, `wss`)

### 2.5d Cross-context exposure list

An explicit list of "places this change is visible from but the diff does not touch", grouped by
execution context: each transport, the Connect task lifecycle callbacks, the recovery/replay
path, the DLQ path, and any client callback thread. Every entry must be reviewed in Step 3.

### 2.5e Test surface & helper inventory

Only when the PR adds or changes test code.

- **Existing-infrastructure inventory:** `rg` for `ConnectTestUtils`, `QuestDBUtils`,
  `baseConnectorProps`, `assertConnectorTaskRunningEventually`, `assertSqlEventually`, the
  `FakeSender`/`FakeContext`/`Recorder` doubles, and `defaultTransports`. A "you stamped
  boilerplate instead of reusing helper X" finding requires X to appear in this inventory.
- **Changed shared helpers as symbols:** a change to `ConnectTestUtils` or a shared fake can
  silently alter every test that uses it — run 2.5b for it.
- **Exercised-symbol map:** for each new or changed test, which production symbols it actually
  exercises, and **on which transports**.

## Step 2.6: Test coverage map (mandatory at every level)

Runs at EVERY level for EVERY PR that touches production code, including PRs with no test
changes — those concentrate the scrutiny here rather than skipping it.

One row per behavioural change, broken down by each new or changed branch, error path, and
boundary case. Per row record:

- **Change:** symbol + specific behaviour/branch.
- **Test:** exact test class and method, found via real `rg`/`fd` searches. Citing a test
  without a recorded search is a skill violation. "Existing tests probably cover it" is banned.
- **Failure link:** what the test asserts and why it fails if this behaviour regresses. "The
  test calls the method" is not a failure link.
- **Transports covered:** which of HTTP / TCP / QWP actually execute this row. A row exercised
  only on the default matrix (HTTP + QWP) is uncovered on TCP — say so.
- **Reachability / population:** the supported operation or configuration that reaches the path,
  and who is affected.
- **Credible regression consequence:** a concrete plausible mutation and what that population
  would observe.
- **Dimensions:** happy, error, boundary, rebalance/restart, concurrency, resource-cleanup —
  each covered / uncovered / N-A with a reason.
- **Disposition:** `COVERED`, `CRITICAL GAP`, `MODERATE GAP`, `ACCEPTED GAP`, or `EXEMPT`.

Rows with no effective test are **UNTESTED**, then classified by evidence:

- **Critical gap (blocking):** the path and population are supported and reachable, a credible
  regression would cause material harm (lost/duplicated rows, a stalled pipeline, DLQ
  misrouting, a broken rebalance, a compatibility break), existing controls do not contain it,
  and the row passes Step 3b. "Bug fix", "public API", or "concurrency" never makes a gap
  Critical by itself.
- **Moderate gap:** bounded regression exposure — most bug fixes without a regression test.
- **Accepted gap:** low-risk or mechanical behaviour where the least invasive meaningful test is
  disproportionate or more fragile than the code under test.
- **Exempt:** verified no-behavioural-change rows.

Publish only admitted gaps; keep COVERED, ACCEPTED, EXEMPT and omitted rows private unless asked.

**A test that runs is not a test that binds.** Before marking a row COVERED, ask whether the
test would still pass if the production change were absent. See "Test doubles must model the
real contract" below — in this repo that is the single most common reason a row is wrongly
marked covered.

## Step 3: Change-specific candidate discovery

Select only roles whose domain is materially touched, obey the level's cap, and launch them as
fresh-context read-only tasks. Agent count is never evidence.

Every selected agent receives: the diff, the Step 2.5 surface map, and the Step 2.6 coverage
map. Write large maps to a file and point tasks at it rather than pasting them into each task.

### Candidate-discovery directive (applies to all agents)

- You are a **hypothesis generator**, not an authority to publish a finding. Output atomic
  propositions for independent falsification. Do not assign severity, propose fixes, write
  persuasive titles, or use "verified", "proved", or "confirmed".
- Cite the exact changed hunk or the unchanged callsite contract allegedly broken.
- Name the **supported-state producer**: the exact user operation, configuration, record shape,
  batch size, or cluster event that creates every trigger condition. If you cannot locate it,
  write `producer: unknown`; do not invent a deployment.
- Give the reachability chain, head observation, same-trigger base observation, user-visible
  symptom, and raw evidence paths/commands. Mark anything not actually checked as `unknown`.
- Actively seek disproof: a guard, a retry, a later validation, an unchanged base behaviour, an
  unsupported configuration, a transport that cannot reach the code.
- Claims containing **never**, **only**, **exactly one**, **no retry** require an exhaustive
  inventory, not one traced path.
- A proposition with no independent consequence is evidence for its parent, not a candidate.
- Two agents repeating the same reasoning are one hypothesis, not corroboration.
- Returning no candidate is valid and preferred to returning a speculative one.

### Agents

**Agent 1 — Correctness & bugs:** NULL handling, edge cases, logic errors, off-by-one, error
paths, exception-type choice. Cross-reference every changed symbol against its callsite
inventory. Always apply the "Kafka Connect contract" and "Delivery invariants" checklists below
— a change that can commit an offset for a row that is not durable, or that can pin a partition's
offset permanently, is a Critical candidate.

**Agent 2 — Concurrency & task lifecycle:** Shared mutable state, missing volatile, and above
all *which thread runs what*. The Connect task thread owns task state; the client may invoke
callbacks on its own dispatcher thread. Check the lifecycle ordering assumptions too: `put`,
`preCommit`, `flush`, `open`, `close`, `stop`, and what Connect guarantees about their order and
budgets.

**Agent 3 — Performance:** The connector's hot path is per-record mapping and per-batch
flushing. State the complexity of every new loop and say what it multiplies by — per record, per
batch, per poll, per flush, once at startup. Per-record allocation on the mapping path is a
finding; a bounded cost at task start is not. Do not import the QuestDB server's zero-GC rules
wholesale: `java.util` collections are normal here off the per-record path.

**Agent 4 — Resource management:** Senders, HTTP/WebSocket connections, and buffers closed on
all paths including error paths; no double-close; no leak when `start()` throws partway.

**Agent 5 — Test review & coverage:** Consume the Step 2.6 map, re-verify every claimed test and
failure link by reading the assertion, and hunt for behavioural changes the map missed. Then run
a **mutation spot-check**: pick the 3-5 most dangerous changed lines and ask per line "which test
fails if this is wrong?" When no assertion would catch a mutation, add an `UNTESTED` row even if
a test nominally executes the line. Explicitly check whether each row is covered on every
transport that reaches it.

**Agent 6 — Code quality & standards:** Code smell, dead code, naming, unclear intent, and
unresolved TODO/FIXME introduced by this PR.

**Agent 7 — PR metadata & conventions:** Title format, description quality, commit messages.

**Agent 9 — Cross-context caller impact:** Walk the 2.5b callsite inventory. For every callsite,
read the calling method plus its callers up two levels and answer: does this caller pass inputs
the new behaviour handles incorrectly? Does it depend on a 2.5c contract the change broke? Is it
in a context — a different transport, a Connect callback, the recovery path, a client callback
thread — where the new behaviour misbehaves even with valid inputs? Output is structured per
callsite with a verdict of SAFE / CANDIDATE / INSUFFICIENT_EVIDENCE.

Select this role whenever changed symbols have out-of-diff callers. In this repo the highest-value
version of it is: **the shared record-mapping code is reached by all three transports — does this
change alter what the legacy transports write?**

**Agent 10 — Fresh-context adversarial:** Receives ONLY the diff and the names of changed files.
No surface map, no contract list, no checklists. Sole instruction: "generate a small set of
falsifiable ways this code could be wrong, and try to disprove each before returning it." Free to
use Read and ripgrep. Each surviving output follows the candidate contract. The point is to
escape the structured frame; a unique hypothesis is not high signal by itself.

**Test-code agents (12-14) — eligible only when the diff adds or changes test code, or claims a
bug fix.** Tests are not second-class code.

**Agent 12 — Test efficacy (adversarial):** Prove each test actually exercises the production
change and could fail if it regressed. Flag vacuous assertions, tests that pass whether or not
the change is present, happy-path-only tests, timing-dependent synchronisation, and
`AssertionError` thrown on a spawned thread where it is swallowed. **Also audit every test double
the test relies on** against "Test doubles must model the real contract" — a test whose fake
cannot produce the state under test is vacuous no matter how many assertions it has.

**Agent 13 — Test-code quality:** Reflection overuse where a helper or constructor reaches the
same state; boilerplate stamped instead of reusing a helper from the 2.5e inventory; javadoc that
merely restates the test name; debugging residue; `@Ignore` without a ticket. Zero-GC rules do
not apply to tests.

**Agent 14 — Regression-test efficacy:** For any PR claiming a fix, verify the regression test
would actually fail without the production change. Use a scratch `git worktree` — never the
primary working tree — run the test at head, then `git checkout <base> -- <files>` and run it
again. Admission requires green-on-head and red-without-fix artifacts.

Combine outputs into a private **candidate ledger**. Split compound narratives into atomic
propositions, deduplicate by proposition plus evidence, record dependencies. A candidate is not
a finding.

## Step 3b: Independently falsify, prove, and admit candidates

`HYPOTHESIS → FALSIFYING → PROVEN → ADMITTED`

Any missing proof, unresolved contradiction, failed reproduction, unsupported producer, or
dependence on an omitted premise ends at `OMITTED`. There is no `DOWNGRADED` state for an
unproven behavioural claim, and "could not disprove" never means `PROVEN`.

At levels 1-3, launch one fresh-context falsifier per atomic candidate. The falsifier receives
only (a) the neutral proposition, (b) target repository and base/head revision identities, and
(c) raw evidence/artifact paths. **Do not send** the discovery narrative, proposed severity,
suggested fix, author identity, other agents' votes, or any statement that the claim was
verified. At level 0 the parent applies the same protocol inline from a blank evidence form
before writing any report prose.

The falsifier's first task is to construct the strongest disproof. Only if the candidate survives
does it assemble affirmative proof.

A behavioural candidate is admitted only when every field is backed by cited evidence:

- **Attribution:** exact changed hunk, or exact unchanged callsite plus the contract this PR changed.
- **Supported-state producer:** exact supported operation/configuration/record shape/cluster event.
- **Reachability:** complete producer-to-symptom path, including callers, retries, and guards.
- **Head observation:** executed trigger and observed output/state at the reviewed revision.
- **Base observation:** the identical trigger at `$BASE`, or `N/A — genuinely new surface` with proof.
- **User symptom:** independently observable consequence.
- **Counterevidence search:** strongest attempted disproof and why it does not apply.
- **Artifact:** exact command, output, environment, and revision identity.

**Runtime evidence is mandatory** for any claim about rebalances, restarts, offset commits,
acknowledgement timing, retries, ordering, or transport behaviour. Static source reading alone
cannot admit one. For findings fully proved by source — a compile error, a direct standards
violation — mark the runtime fields `N/A — static` and cite the complete source proof.

Special burdens:

- A claim containing a universal negative needs an exhaustive inventory plus an executed probe.
- A concurrency or ordering claim must force or observe the interleaving; timing prose is not evidence.
- A regression-test claim must run the test on head and against the reverted production hunk.
- A transport-parity claim must be executed on **each** transport it names, not inferred from one.
- If a parent premise is omitted, omit every candidate that depends on it.

If required execution is impossible, record the limitation privately and omit the candidate.
Never fall back from failed execution to confident prose.

After a candidate satisfies the schema, apply these checks:

1. **Read the actual source at the exact lines cited.** Do not rely on the agent's description.
2. **Trace the full path**, including polymorphic dispatch — the task dispatcher delegates to one
   of two `SinkTask` implementations, so "the task does X" must name which one.
3. **For resource-leak claims:** trace every allocation to its close on all paths, and verify the
   intervening code can actually throw.
4. **For performance claims:** state the multiplier and what it multiplies, or the fixed bound.
   A claim with neither is not verified.
5. **For cross-context findings:** re-read the callsite in full including its callers, and
   confirm the behaviour is reachable from a production path on a supported transport.
6. **For test-efficacy candidates:** re-read the cited assertion in full context and confirm it
   can fail for the claimed regression. Use a scratch worktree; never mutate the primary tree.
7. **For coverage-gap candidates:** try to falsify the risk with existing indirect assertions,
   guards, type guarantees, or downstream validation before assigning any severity.
8. **Verify the conjunction, not just the links.** A multi-step candidate is only as true as its
   weakest step. Identify the load-bearing step — usually "this supported state can actually
   occur" — and falsify that first.
9. **Derive a fix only after admission**, then verify it compiles and closes the window.
10. **Determine net user impact, then classify.**

    **(a) Net user impact — answer all five, in order:**
    - **Population** — who reaches it: every pipeline, every pipeline on one transport, one
      record shape, one configuration, an operator-only path. "Any user in principle" is not a
      population.
    - **Delta vs base** — what that population observes differently from `$BASE` for the identical
      executed trigger.
    - **Magnitude and frequency** — per record, per batch, per rebalance, per restart, once ever.
    - **Offsets** — what recovers this downstream before the user sees anything: a retry, a
      later validation, redelivery after an uncommitted offset, `DEDUP UPSERT KEYS` on the target
      table, a documented operator procedure. Name the offset, or write "none found, searched <where>".
    - **Net** — exactly one of **net-negative** (admissible), **net-neutral**, or **net-positive**
      (both omitted from PR findings).

    **(b) Classify ledger entries** as ADMITTED in-diff, ADMITTED out-of-diff-breakage, OMITTED
    pre-existing/not-attributed, OMITTED false, or OMITTED unverified.

Keep omitted candidates and their disproofs private. Do not publish a retracted or "possible
issue" section, and do not report candidate counts. **OMITTED pre-existing/not-attributed** is
the one exception: an entry with producer, reachability, and observation all proved leaves as a
Step 4 adjacent issue draft.

## Review checklists

### Kafka Connect contract

These are the framework facts most often assumed wrongly. Verify against the Kafka version in
`pom.xml` rather than memory when a finding depends on one.

- **`put()` is called with an empty collection** on an idle poll. Code that only runs inside the
  non-empty branch will not run when the pipeline is quiet.
- **`preCommit()` runs before `close()`** on partition revocation. Anything `close()` learns
  cannot influence the offsets just committed — a drain there buys nothing and delays the
  rebalance for the whole group.
- **The map returned by `preCommit()` is what Connect commits.** Withholding an offset causes
  redelivery, not loss. Committing an offset for a row that is not durable causes loss.
- **Exception type decides the outcome.** `RetriableException` makes Connect pause and redeliver
  the same batch; any other exception fails the task and is not covered by `errors.tolerance`.
  `errors.tolerance` covers the converter and transform stages plus the errant-record reporter —
  it does **not** catch an exception thrown out of `put()`.
- **`ErrantRecordReporter.report()` returns a future completed on broker ack**, not an
  already-completed one. Any logic — or test double — that assumes immediate completion is wrong.
- **Connect keeps its own record of paused partitions.** `context.pause()` is remembered in
  `WorkerSinkTaskContext` and re-applied when those partitions are assigned again; the set
  outlives a revocation. A task-local "paused" flag must not be the only record, or partitions
  come back paused with nothing able to resume them.
- **The default assignor is eager**, so every rebalance revokes the entire assignment. Logic that
  only handles partial revocation is untested in the common case.
- **`task.shutdown.graceful.timeout.ms` (default 5s) is the budget for ALL tasks on a worker**,
  and `cancel()` cannot interrupt a task blocked in a wait. A shutdown path that blocks for tens
  of seconds holds its consumer and stalls the group well past the budget.
- **`max.poll.interval.ms` (default 300s) bounds total blocking inside `put()`**, including every
  client-side wait it can perform in one call.
- **KIP-793 original coordinates** (`originalTopic`, `originalKafkaPartition`,
  `originalKafkaOffset`) require Connect 3.6+; they are what makes offset tracking correct under
  an SMT that rewrites the topic.

### Delivery invariants

- Never commit an offset for a row that is not durable. That is silent data loss and is Critical.
- Withholding offsets is safe — it produces duplicates, which the documented at-least-once model
  plus `DEDUP UPSERT KEYS` covers. **But a withheld offset that can never advance is a stalled
  pipeline**, and a stall while the task still reports `RUNNING` is worse than a crash because
  nothing alerts. Treat any state that can pin an offset permanently as Critical.
- A record must end in exactly one terminal state: written, or reported to the DLQ. Both, or
  neither, is a defect.
- Never send a valid record to the DLQ to escape an ambiguous error. Misrouting good data is
  data loss with extra steps; failing loudly is better.

### Transport parity

The connector serves three transports through largely shared code: `http`/`https` and
`tcp`/`tcps` via the legacy task, `ws`/`wss` (QWP) via the QWP task. Selection is driven purely
by the configuration-string prefix.

- **Every finding must state which transports it affects.** A change to shared mapping code
  reaches all three.
- **Confirm which task actually serves a transport** before attributing behaviour. The two tasks
  have different delivery models; only one has the FSN ledger, replay isolation, and offset
  clamping.
- **Known asymmetries** — check these before claiming a divergence is new:
  - `cancelRow()` is illegal on TCP, so a partial row cannot be discarded there and
    DLQ-and-continue is impossible on that transport.
  - The QWP client remembers each column's type for the life of the connection; HTTP does not,
    and lets the server reject instead.
  - ILP over TCP is fire-and-forget: no per-row server rejection, so nothing can be isolated or
    routed to a DLQ from a server error.
  - The client publishes on its own auto-flush cadence, which is far tighter than the connector's
    checkpoint. Anything that assumes the connector controls when data is published is suspect.
- **A behaviour difference between transports for the same configuration is a finding** unless it
  is documented, because it silently changes what lands in the database when a user migrates.

### Test doubles must model the real contract

The most common reason a defect ships green in this repo is a fake that cannot express the state
the defect needs. When reviewing or relying on a test double, ask: **which production state can
this fake never produce?** Then check whether the change under review lives in that state.

Known traps, all of which have hidden real defects:

- A sender fake whose flush always returns a valid sequence number cannot express "the client
  already published this on its own cadence".
- A context fake that returns an already-completed DLQ future cannot express the normal case,
  where the broker has not acked yet.
- A context fake that counts `pause`/`resume` calls without tracking *which* partitions remain
  paused cannot express a pause that survives a rebalance.
- A recording proxy that only records some sender methods makes assertions about the others
  vacuous.

A test built on such a fake is not evidence of coverage. Say so in the Step 2.6 row.

### Running tests in this repo

Commands the review may need to execute:

```bash
# unit + embedded tests (default transport matrix: HTTP + QWP)
mvn -o -pl connector test -Djacoco.skip=true

# opt TCP back in
mvn -o -pl connector test -Dquestdb.test.transports=HTTP,QWP,TCP -Djacoco.skip=true

# scratch worktree for revert-the-fix verification
git worktree add --detach <scratch-path> <rev>
```

Gotchas that will otherwise cost time or produce a wrong verdict:

- **`git-commit-id-plugin` fails inside a linked worktree** (`Missing unknown <sha>`); add
  `-Dmaven.gitcommitid.skip=true` there.
- **A green build is not proof tests ran.** A long failure message — notably one embedding a
  stack trace — corrupts surefire's fork channel, which discards every result for that class and
  reports `Tests run: 0` with `BUILD SUCCESS` and exit 0. Always read the test count, not the
  build result.
- **CI does not run integration tests.** `.github/workflows/ci.yml` runs `mvn -B package`, which
  never reaches the `integration-test` phase; `it.yml` is `workflow_dispatch` only. An
  integration test therefore cannot be cited as a CI gate — say so when coverage depends on one.
- **Testcontainers leaves QuestDB data owned by the container's uid**, so the files it writes
  into a bind-mounted scratch directory may not be removable by the user running the review.
- Always remove scratch worktrees and containers you create.

### Correctness & bugs

- NULL handling, including tombstone records (`record.value() == null`)
- Edge cases and error paths; off-by-one; wrong operator precedence
- Exception type chosen deliberately, per the Connect contract above
- Config validation that fails fast at task start rather than mid-stream

### Concurrency

- Unsynchronized shared mutable state; missing `volatile`; unsafe publication
- Which thread runs each callback — task thread vs client dispatcher thread
- For every changed symbol, whether it is now reached from a context where the previous
  assumptions do not hold

### Resource management

- Senders and connections closed on all paths, including when `start()` throws partway
- No double-close; no unbounded retention of records after they are durable

### Test review

- **Coverage gaps are impact- and proportionality-assessed** — consume the Step 2.6 map.
- **Transport dimensions:** a change to shared code tested on one transport is a gap on the
  others. Name the untested transports.
- **Lifecycle dimensions:** rebalance, task restart, and connector restart are execution modes
  here in the same way WAL/non-WAL are for the server. A delivery change tested only in
  steady state is a gap.
- **Error-path, boundary, and batch-size coverage:** batch sizes that cross the client's
  auto-flush threshold behave differently from small ones; a test using 3 records may not reach
  the code a real 500-record poll does.
- **Regression tests:** if this PR fixes a bug, is there a test that fails without the fix?

### Code quality

- Code smell: overly complex methods, deep nesting, unclear intent, dead code
- Naming: `is`/`has` prefixes for booleans; members grouped and ordered consistently
- No debugging residue; no `@Ignore` without a ticket
- New TODO/FIXME introduced by this PR: is it deferred work that should block, or an accepted
  limitation? Pre-existing ones that were merely moved are not findings.

### Commit messages

- Conventional Commits title, imperative mood
- Body explains user impact and reasoning, not just mechanism

## Step 4: Output

Present only **ADMITTED** findings. Omitted candidates, disproofs, retractions, agent counts and
candidate counts never appear. It is valid to report no findings. The one exception is the
**Adjacent findings** section, which carries proved pre-existing bugs as issue drafts.

**Proportionality.** Keep the report actionable in one sitting. If a normal-sized PR yields more
than about seven findings, re-run the admission gate and remove dependent, duplicate, and
not-attributed items. Review depth is demonstrated by evidence, not report length.

**Every finding opens with three one-line summaries, before any prose:**

- **Problem:** what is wrong. ≤ 12 words. No mechanism or fix.
- **Net impact:** supported population and magnitude. ≤ 12 words.
- **Evidence:** the decisive artifact, including the reviewed revision identity.

Write these last from the completed admission form, never first from a hunch. Then give only the
minimal producer → path → symptom trace, base comparison, and suggested fix.

```
Problem: Offset committed before the row is acknowledged.
Net impact: Silent row loss on every task restart under load.
Evidence: OffsetClampTest red at abc123, green at base def456.

Problem: Backpressure pause survives a rebalance.
Net impact: QWP pipelines stop consuming after any rebalance.
Evidence: embedded rebalance probe at abc123; 0/10 records for 125s.
```

### Severity classification (impact-first)

Severity is a function of **what the user loses**, not of which checklist the finding came from.
Classify by the worst *user-visible* consequence on a *reachable* path. Do not classify up "to
be safe".

**"The user" means someone running a Kafka→QuestDB pipeline, or the operator of a Connect
cluster.** It does not mean a developer of this connector or a CI job. A finding whose only
affected population is the team is never Critical.

**The Critical test — name the symptom.** A finding is Critical only if you can complete
*"Because of this, the user sees ___"* with something they would actually observe:

- **lost or duplicated data beyond the documented model** — offsets committed for rows that were
  never written, records that reach neither the table nor the DLQ, a valid record misrouted to
  the DLQ;
- **a stalled or dead pipeline** — a task that fails on every restart, offsets that can never
  advance, partitions that are never resumed, an unbounded retention that ends in OOM. A stall
  while the task reports `RUNNING` counts, and is worse than a crash;
- **a broken consumer group** — a rebalance or shutdown that exceeds Connect's budgets and
  disrupts other members;
- **a broken or misleading failure mode** — an operation that fails with no error or the wrong
  error, an exception swallowed so failure looks like success, a fault that cannot be diagnosed;
- **a compatibility break** — a configuration key whose semantics changed, a transport whose
  behaviour changed for existing users, a schema mapping that now writes different values;
- **a throughput regression the user can feel** — per the magnitude rule;
- **an admitted Critical coverage gap** — reachable supported path, named population, credible
  material regression, no containing control.

**Every completion needs a trigger:** the concrete record shape, batch size, configuration, or
cluster event a user can produce. "Could theoretically lose data" is not evidence.

**Magnitude rule for performance.** Cost blocks only when it is user-observable. Per-record work
on the mapping path, or per-batch work that scales with batch size, is Critical when it moves
throughput. A bounded cost at task start, per connector restart, or per config parse is Moderate.
State the multiplier and what it multiplies, or the fixed bound.

**Out of scope — not findings:**

- **Merge mechanics.** Branch state, labels, and anything true only of the PR's in-flight state.
- **Tautologies.** If the finding would appear on every PR of this shape, it describes the
  workflow, not this change.
- **Overridden project decisions.** Where the project's own tooling or documented convention
  explicitly permits something, that is a decision, not an oversight.
- **Upstream dependency content.** Behaviour inside a released `questdb-client` or `kafka-clients`
  version bumped by this PR. The exception is an integration defect at a callsite in this diff.

**Moderate.** Admitted, attributable defects with bounded or developer-facing impact: a concrete
standards violation on changed lines, a proved weak test, missing internal-path coverage, a
documentation defect that leaves a real limitation unstated, or a bounded off-path cost.

**Minor.** Cosmetics on changed lines.

### Critical

Blocking issues, ordered worst user impact first. Each must include: the three summary lines; the
net determination (population, delta vs base, magnitude/frequency, offsets, net-negative); exact
file paths and line numbers; the symptom sentence with its trigger; whether it is in-diff or
out-of-diff-breakage; the code-path trace; **base behaviour for the identical trigger** (or
`N/A — new surface` with proof); the affected transports; and a suggested fix written to be
applied in this PR.

### Moderate

Non-blocking admitted issues worth fixing, with the same three summary lines and decisive
evidence. Dynamic behavioural speculation is not allowed here.

### Minor

Concrete cosmetics on changed lines. Non-blocking.

### Adjacent findings (not blocking — file as GitHub issues)

Bugs that already exist on the merge base, found in code this review visited, which this PR does
not introduce or worsen. They never appear under Critical/Moderate/Minor and never influence the
verdict — but discarding them wastes an investigation already paid for.

Held to the same evidence bar as a published finding: only a candidate that reached **OMITTED
pre-existing/not-attributed** with producer, reachability, and observation proved qualifies.

Report each as a ready-to-file issue draft: **Problem** (≤ 12 words, doubles as the title),
**Net impact**, **Location**, **Symptom**, **Reachability**, **Suggested fix**, and **Severity if
filed standalone**. Offer to file them; do not file anything without being asked.

If one is severe enough that shipping this PR without it is genuinely unsafe — because this PR
moves code onto a path where the pre-existing bug now fires — then it is not adjacent: it is
out-of-diff-breakage and belongs under Critical.

### Coverage map

State the test-gate result and the number of **admitted** coverage gaps only. Render admitted
rows with their recorded search and failure link. Keep the full matrix private unless asked.

### Summary

- **Verdict**, exactly one of:
  - **approve** — no open Critical findings and the test gate passes. This is the expected
    outcome for competent work; withholding it when both gates pass is itself a review failure.
  - **approve with comments** — both gates pass; name the Moderate items you want addressed.
  - **request changes** — at least one Critical is open, or the test gate fails.
  - **needs discussion** — the change requires a product or compatibility decision.
- **Correctness gate (hard rule):** the verdict cannot be "approve" while any ADMITTED Critical
  finding remains open. Before finalizing, rerun the admission audit from evidence fields rather
  than report prose: strongest attempted disproof per finding; a supported producer for every
  trigger state; confirmation the falsifier received no narrative, severity, fix, or votes; an
  executed artifact for every dynamic claim plus its same-trigger base result; removal of every
  item whose parent premise was omitted; severity assigned only after admission.
- **Test gate (hard rule):** fails only while an ADMITTED Critical coverage gap remains open.
  Zero test changes or a bug-fix label triggers the Step 2.6 analysis but never automatically
  forces `request changes`.
- State the Step 2.4 dependency provenance verdicts, one line per bump. If a version moved and no
  verdict is stated, the scope of the review is unknown and the report is incomplete.
- State which transports the review actually exercised, and which it did not.
- State only the admitted split: in-diff / out-of-diff-breakage. At levels 0-1, describe the
  limited callsite analysis rather than implying a clean bill of health.
- Do **not** state agent counts, candidate counts, rejected counts, or retraction history.
