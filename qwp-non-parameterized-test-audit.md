# QWP audit of non-parameterized embedded tests

## Recommendation

Parameterize 35 of the 42 plain `@Test` methods in
`QuestDBSinkConnectorEmbeddedTest` over
`ConnectTestUtils#defaultTransports`. Keep seven tests single-run because they
either test an explicitly legacy transport mechanism or fail before the sink
task's transport-specific data path can run.

## Implementation status

Implemented in the current working copy. All 35 accidental omissions now use
the HTTP+QWP default transport matrix. The class now contains 93 parameterized
tests and the seven deliberate plain tests listed below. The implementation also
renamed `testbadColumnType_noDLQ` to `testBadColumnType_noDLQ`.

This is mostly migration debt. Every plain test below predates the QWP transport
matrix added by commits `1cd8e3d` and `63083fc` on 2026-08-14. That migration
converted tests that were already parameterized over the old HTTP/TCP boolean,
but did not classify the existing plain tests. Today,
`baseConnectorProps(..., true)` still selects HTTP only; it never selects QWP.

## Verdict rules

- **Accidental**: the test sends a record into the sink task and its contract is
  supposed to hold for both HTTP and QWP. Use `@ParameterizedTest`,
  `@MethodSource("io.questdb.kafka.ConnectTestUtils#defaultTransports")`, and the
  `Transport` overload of `baseConnectorProps`.
- **Deliberate**: the behavior belongs to one named legacy transport, or the
  failure occurs in shared configuration/SMT code before the QWP task data path
  receives a record. Repeating such a test with a `ws::` configuration would not
  test QWP delivery.

The verdict describes whether the test should remain non-parameterized now. It
does not claim knowledge of the original author's intent.

## Inventory and classification

Source line numbers and current-scope values capture the pre-implementation
working copy audited on 2026-08-31. Parameterization shifts the later lines and
changes the 35 accidental entries from HTTP to HTTP+QWP.

| # | Test (line) | Scope at audit | Verdict | Evidence and recommended action |
|---:|---|---|---|---|
| 1 | `testTombstoneRecordFilter` (326) | HTTP | **Accidental** | Tombstone skipping and the following good row pass through different task implementations. Parameterize HTTP+QWP to cover QWP ledger admission around a skipped record. |
| 2 | `testDeadLetterQueue_invalidTableName` (516) | HTTP | **Accidental** | Client-side `InvalidDataException` handling is implemented separately by `LegacyQuestDBSinkTask` and `QwpSinkTask`. Both claim per-record DLQ support; parameterize. |
| 3 | `testDeadLetterQueue_invalidColumnName` (547) | HTTP | **Accidental** | Same separate client-side DLQ paths as #2. Parameterize. |
| 4 | `testDeadLetterQueue_unsupportedType` (577) | HTTP | **Accidental** | The record handler rejects the value, after which each task has its own DLQ bookkeeping. Parameterize. |
| 5 | `testDeadLetterQueue_emptyTable` (607) | HTTP | **Accidental** | Empty dynamic table-name validation is common, but QWP has separate retained-record and DLQ-completion state. Parameterize. |
| 6 | `testDeadLetterQueue_badColumnType` (637) | HTTP | **Accidental** | This is a server-side schema rejection. HTTP retries rows individually; QWP uses terminal-error replay isolation and FSN/ACK bookkeeping. QWP is a required leg, not an equivalent implementation detail. |
| 7 | `testDeadLetterQueue_sendBatchOnError` (779) | HTTP | **Accidental** | The option explicitly has different implementations: an HTTP in-flight batch versus QWP's unresolved retained error window. Add QWP, but assert its documented unresolved-window semantics rather than assuming an already ACKed prefix must enter the DLQ. |
| 8 | `testbadColumnType_noDLQ` (821) | HTTP | **Accidental** | Without a reporter, QWP deliberately converts a terminal schema rejection into task failure. The existing eventual `FAILED` assertion is appropriate for both transports. Parameterize and rename to `testBadColumnType_noDLQ`. |
| 9 | `testRetrying_badDataStopsTheConnectorEventually_tcp` (878) | TCP | **Deliberate** | It tests the legacy TCP reconnect/retry counter and is named for TCP. QWP has a different acknowledgement/error model. Keep single, but use `Transport.TCP` instead of the boolean helper for clarity. |
| 10 | `testRetrying_badDataStopsTheConnectorEventually_http` (911) | HTTP | **Deliberate** | `max.retries`/`remainingRetries` is legacy-task machinery; `QwpSinkTask` does not use it for terminal schema errors. Keep single and use `Transport.HTTP` explicitly. A QWP terminal-error test is a separate contract, partly covered by #8. |
| 11 | `testExactlyOnce_withDedup` (1122) | HTTP | **Accidental** | Its comment excludes TCP, not QWP. `defaultTransports()` now already excludes TCP, while QWP's ACK-based offset handling makes restart/dedup coverage important. Add a QWP leg; because this test sends one million rows and restarts QuestDB repeatedly, consider a smaller focused QWP variant instead of blindly doubling this stress test. |
| 12 | `testContentBasedRouting_extractFromValueStruct` (1470) | HTTP | **Accidental** | The SMT changes the table name, then the transformed rows are ingested. QWP dynamic-table ingestion is part of the observed success. Parameterize. |
| 13 | `testContentBasedRouting_extractFromKey` (1509) | HTTP | **Accidental** | Same reasoning as #12, including the non-default key converter. Parameterize. |
| 14 | `testExtractKafkaIngestionTimestampAsField_designated` (1661) | HTTP | **Accidental** | The SMT-injected timestamp becomes the QWP designated timestamp. Parameterize. |
| 15 | `testExtractKafkaIngestionTimestampAsField_nondesignated_schemaless` (1700) | HTTP | **Accidental** | The transformed timestamp is sent as an ordinary timestamp column; QWP type encoding is relevant. Parameterize. |
| 16 | `testSchemalessFloatArraySupport` (2329) | HTTP | **Accidental** | Array encoding is transport-visible and existing schemaful array tests already use the HTTP+QWP matrix. Parameterize. |
| 17 | `testSchemalessFloatArraySupport_floatFollowedByInt` (2349) | HTTP | **Accidental** | Mixed numeric coercion feeds the emitted array. Parameterize to cover QWP array encoding. |
| 18 | `testSchemalessFloatArraySupport_intFollowedByFloat` (2369) | HTTP | **Accidental** | Same as #17 with the inference order reversed. Parameterize. |
| 19 | `testIntegerArrayRejection` (2390) | HTTP | **Accidental** | Rejection happens in connector record conversion, and task-failure propagation exists separately in the QWP task. Parameterize. |
| 20 | `testArrayWithSkipUnsupportedTypes` (2455) | HTTP | **Accidental** | After skipping the unsupported field, the remaining row is delivered. QWP must be shown to receive the valid remainder. Parameterize. |
| 21 | `testOrderBookToArraySMT_schemaless` (2574) | HTTP | **Accidental** | The SMT succeeds and emits 2D arrays to QuestDB. QWP array delivery is exercised only if this is parameterized. |
| 22 | `testSchemaless2DArraySupport` (2642) | HTTP | **Accidental** | Direct schemaless 2D-array ingestion is a QWP data-type contract. Parameterize. |
| 23 | `testSchemaless3DArraySupport` (2662) | HTTP | **Accidental** | Direct schemaless 3D-array ingestion is a QWP data-type contract. Parameterize. |
| 24 | `testJaggedArrayRejection` (2718) | HTTP | **Accidental** | The connector rejects the record during row conversion; QWP task-failure propagation should be covered. Parameterize. |
| 25 | `testOrderBookToArraySMT_intAndFloatCoercion` (2809) | HTTP | **Accidental** | The transformed 2D array is successfully ingested. Parameterize to cover QWP encoding. |
| 26 | `testOrderBookToArraySMT_missingSourceField` (2847) | HTTP | **Accidental** | The SMT omits one target and the partial transformed row is still ingested. Parameterize. |
| 27 | `testOrderBookToArraySMT_emptySourceArray` (2886) | HTTP | **Accidental** | The empty array is omitted and the remaining row is delivered. Parameterize to cover QWP's resulting row. |
| 28 | `testOrderBookToArraySMT_nullValueInStruct` (2922) | SMT failure before task delivery | **Deliberate** | With `errors.tolerance=none`, the SMT throws before either sink task receives a row. A QWP leg would only duplicate Kafka Connect transformation failure. Keep single; a focused SMT unit test would be clearer. |
| 29 | `testOrderBookToArraySMT_targetCollidesWithExisting` (2942) | HTTP | **Accidental** | The replacement array is successfully delivered, so QWP encoding and schema interaction matter. Parameterize. |
| 30 | `testMarketData_orderBookToArray_withTimestamp_schemaless` (2983) | HTTP | **Accidental** | This combines symbols, a designated string timestamp, and 2D arrays—several known HTTP/QWP differences. Parameterize. |
| 31 | `testMarketData_orderBookToArray_stringEncodedPrices_schemaless` (3055) | HTTP | **Accidental** | After SMT coercion, the row still exercises QWP symbols, timestamp, and 2D-array delivery. Parameterize. |
| 32 | `testComposedTimestamp_emptyFieldNameRejected` (3186) | Shared startup validation | **Deliberate** | `QuestDBSinkTask.start()` constructs `QuestDBSinkConnectorConfig` before choosing the legacy or QWP delegate. The failure therefore precedes transport selection. Keep single or move to a config unit test. |
| 33 | `testComposedTimestamp_duplicateFieldNameRejected` (3201) | Shared startup validation | **Deliberate** | Same pre-dispatch validation boundary as #32. Keep single or move to a config unit test. |
| 34 | `testEnvVarInterpolation_undefinedVariable` (3216) | Shared startup resolution | **Deliberate** | `ClientConfUtils.resolveConfString()` expands variables before `QuestDBSinkTask` calls `isQwp()` and selects a delegate. The undefined variable fails before QWP can be selected. Keep one integration leg; add protocol-specific interpolation tests only at unit level if needed. |
| 35 | `testStructArrayExplodeSMT_schemaless` (3282) | HTTP | **Accidental** | The successful SMT output contains four arrays that are delivered to QuestDB. Parameterize for QWP array encoding. |
| 36 | `testStructArrayExplodeSMT_intAndFloatCoercion` (3309) | HTTP | **Accidental** | The transformed/coerced arrays are successfully ingested. Parameterize. |
| 37 | `testStructArrayExplodeSMT_missingSourceField` (3347) | HTTP | **Accidental** | A partial transformed row is delivered. Parameterize to cover QWP. |
| 38 | `testStructArrayExplodeSMT_emptySourceArray` (3387) | HTTP | **Accidental** | The empty source is omitted and the remaining row is delivered. Parameterize. |
| 39 | `testStructArrayExplodeSMT_nullValueInStruct` (3423) | SMT failure before task delivery | **Deliberate** | Like #28, the SMT failure with `errors.tolerance=none` occurs before the sink task data path. Keep single; prefer a focused SMT unit test. |
| 40 | `testStructArrayExplodeSMT_targetCollidesWithExisting` (3443) | HTTP | **Accidental** | The replacement arrays are successfully delivered. Parameterize. |
| 41 | `testStructArrayExplodeSMT_stringEncodedValues` (3484) | HTTP | **Accidental** | The SMT coerces strings and the resulting arrays are ingested. Parameterize. |
| 42 | `testMarketData_structArrayExplode_withTimestamp_schemaless` (3512) | HTTP | **Accidental** | This combines symbols, designated timestamp parsing, and four array columns. It is high-value QWP compatibility coverage. Parameterize. |

## Evidence behind the boundary

- `ConnectTestUtils.baseConnectorProps(..., boolean)` maps `true` to
  `Transport.HTTP` and `false` to `Transport.TCP`. QWP is reachable only through
  the `Transport` overload.
- `ConnectTestUtils.defaultTransports()` runs HTTP and QWP by default; TCP is an
  opt-in legacy leg.
- `QwpSinkTask.put()` has its own retained-record, client-side DLQ, server
  terminal-error, and ACK bookkeeping. Therefore successful delivery and task
  failure after task admission are not adequately covered by an HTTP-only test.
- `QuestDBSinkTask.start()` resolves shared configuration before choosing its
  delegate. Tests that fail during that shared phase do not become QWP tests by
  swapping `http::` for `ws::`.
- Kafka Connect applies SMTs before calling `SinkTask.put()`. The two null-value
  SMT tests fail at that boundary, so duplicating them by transport adds no QWP
  data-path coverage.

## Suggested execution order

1. Parameterize the eight tombstone/DLQ tests (#1-#8) first. They cover the
   largest semantic difference between the legacy task and QWP.
2. Add focused QWP restart/dedup coverage for #11 without automatically doubling
   its one-million-row workload.
3. Parameterize the successful timestamp, routing, array, and SMT-output tests.
4. Keep the seven deliberate cases single-run, but replace boolean transport
   arguments in #9 and #10 with explicit enum values and document the boundary
   in the test.

## Completeness check

- Original plain `@Test` methods inventoried: **42**
- Accidental QWP omissions implemented: **35**
- Deliberate single-run tests: **7**
- Parameterized tests before implementation: **58**
- Parameterized tests after implementation: **93**

## Validation

- All 35 newly added QWP legs pass, including the one-million-row
  `testExactlyOnce_withDedup` restart/deduplication case.
- Focused HTTP regression coverage passes for the DLQ fixtures and the
  no-DLQ task-failure case changed while enabling QWP.
- Focused `RecordToRowHandler` and `QwpSinkTask` unit tests pass.
- `mvn -pl connector -DskipTests test-compile` and `git diff --check` pass.
