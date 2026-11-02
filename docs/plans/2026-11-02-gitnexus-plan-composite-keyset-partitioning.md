# GitNexus Engineering Plan

> Task: Extend the existing JDBC `dynamic-keyset` sparse scan from one partition column to ordered composite-key partitioning without regressing single-column jobs.
> Evidence verified at commit `10849fb3cd9fc096288c9c4ea1a613cf67a792f3`; GitNexus index not used (CLI/index unavailable, source-derived fallback mode).
> Evidence provenance schema 2; global dirty digest `d573fda36db8498ad2065a60cebc7c3315a967760004730b981dc5d91295a945`; cited-path manifest 16 sorted entries; exact generated plan path excluded.

## 1. Objective

Add ordered composite-key sparse partitioning for bounded JDBC scans, for example `(tenant_id, order_id)`, while retaining the existing real-row-position discovery algorithm, filter participation, retry/timeout controls, single-column SQL compatibility, and flat serializable split transport.

Acceptance criteria:

- Existing `scan.partition.column='id'` jobs keep their current behavior and parameter layout.
- A new ordered multi-column configuration produces exhaustive, non-overlapping `[lower, upper)` tuple ranges.
- Sparse values in any key component do not create empty numeric-distance partitions.
- Every configured column is schema-validated as present and `NOT NULL`.
- Discovery predicates and data predicates use the same database lexicographic ordering.
- MySQL integration tests prove correctness for sparse mixed-type composite keys and pushed-down filters.
- The production guide states index, consistency, recovery, and MySQL-compatible database validation boundaries.

## 2. Current Behaviour

- [verified] `JdbcConnectorOptions` exposes only singular `scan.partition.column`; `JdbcReadOptions` stores a single `String partitionColumnName` (`JdbcConnectorOptions.java:83-155`, `JdbcReadOptions.java:27-74`).
- [verified] `JdbcDynamicTableFactory` requires the singular column for `dynamic-keyset`, checks only that one field exists and is non-nullable, and limits the strategy to the MySQL dialect (`JdbcDynamicTableFactory.java:145-169,347-402`).
- [verified] `JdbcDynamicTableSource` builds one-column data predicates as `key >= ? AND (? IS NULL OR key < ?)` and places key parameters before pushed-down filter parameters (`JdbcDynamicTableSource.java:150-176,197-205`).
- [verified] `JdbcDynamicKeysetParametersProvider` discovers one scalar minimum, advances with `ORDER BY key LIMIT chunkSize,1`, falls back to the next distinct scalar value, and emits `[start,end,end,filter...]` (`JdbcDynamicKeysetParametersProvider.java:112-174,500-545`).
- [verified] Discovery owns one reusable connection/three prepared statements, retries transient failures, reconnects on connection failures, and bounds both query and total discovery time (`JdbcDynamicKeysetParametersProvider.java:213-412`).
- [verified] Each discovered parameter row becomes a serializable `JdbcParameterValuesInputSplit`; task retry reruns the complete split query (`JdbcRowDataInputFormat.java:289-305`, `JdbcParameterValuesInputSplit.java:25-43`).
- [verified] The working tree already contains unsigned `BIGINT`/`BigInteger` parameter binding changes in `JdbcRowDataInputFormat` and its test; this plan must preserve them.

## 3. Relevant Architecture

The execution path is:

1. `JdbcDynamicTableFactory` validates table options and resolves them into `JdbcReadOptions`.
2. `JdbcDynamicTableSource.getScanRuntimeProvider()` renders one reusable data SQL template and supplies a `JdbcParameterValuesProvider`.
3. `JdbcRowDataInputFormat.createInputSplits()` invokes the provider once and materializes all parameter rows as input splits.
4. `JdbcDynamicKeysetParametersProvider` queries actual ordered keys to create split boundaries before scan tasks execute.
5. Each task binds its flattened parameter array and executes the common SQL template.

[inferred] The main correctness boundary is the implicit contract between SQL placeholder order in `JdbcDynamicTableSource` and flattening order in `JdbcDynamicKeysetParametersProvider`; composite support should centralize that contract instead of duplicating it.

[verified] Filter parameters have different relative placement in discovery SQL (filters before key cursor) and data SQL (range before filters). A shared SQL/layout helper therefore needs explicit methods for both layouts rather than one generic concatenation routine.

## 4. GitNexus Findings

GitNexus was unavailable, so all findings below are source-derived fallback findings from targeted `rg`, exact source reads, `git diff`, and Maven baseline execution.

- Primary symbol: `JdbcDynamicKeysetParametersProvider`; direct source callers are `JdbcDynamicTableSource` and its unit tests.
- Primary symbol: `JdbcDynamicTableSource.getScanRuntimeProvider`; it owns the data SQL template, range predicate insertion, filter ordering, and provider construction.
- Primary symbol: `JdbcDynamicTableFactory`; it owns option compatibility, schema field lookup, nullability validation, and MySQL-only gating.
- Direct configuration dependency: `JdbcReadOptions`; equality and builder semantics must include ordered column lists.
- Direct transport dependency: `JdbcParameterValuesInputSplit`; no representation change is required if tuple boundaries are flattened before split construction.
- Direct binding dependency: `JdbcRowDataInputFormat.open`; every tuple component must remain one of its supported scalar parameter types.
- Existing test boundaries: core provider/factory tests and MySQL factory/source integration tests.
- Baseline command `mvn -q -pl flink-connector-jdbc-core ... -Dtest=JdbcDynamicKeysetParametersProviderTest,JdbcRowDataInputFormatTest test` completed successfully after sandbox write access was granted.

## 5. Statement-Level PDG Findings

No PDG layer was available. The following constraints are source-derived rather than reconstructed graph edges:

### `JdbcDynamicKeysetParametersProvider.discoverParameterValues`

- [verified] Cursor advancement happens only after a boundary query succeeds; reconnect retries the same cursor (`lines 129-146,213-217`).
- [verified] `end == start` enters a next-distinct fallback, preventing an infinite loop for duplicate partition values (`lines 137-143`).
- [inferred] Composite implementation must retain this fallback because callers may configure a non-unique tuple even when a primary/unique tuple is recommended.
- [verified] Final split is represented by a null upper bound (`lines 133-135,140-142`); the composite representation should preserve a null tuple as the sole unbounded marker.

### `JdbcDynamicTableSource.getScanRuntimeProvider`

- [verified] A pushed global limit disables dynamic splitting and creates one parameter set (`lines 152-157`). Composite support must retain that control branch.
- [verified] Range predicates are appended before resolved filter predicates (`lines 171-205`); provider output must keep range values before `pushdownParams`.

### `DiscoveryQueryExecutor.query`

- [verified] Prepared statements are reused and cleared before every execution, with one-based scalar binding via `setObject` (`lines 248-270`).
- [inferred] Generalizing the result from one scalar to an ordered tuple must not change retry classification, statement reuse, timeout calculation, or connection invalidation ordering.

Performance observations:

- [verified] Boundary discovery performs approximately one ordered index walk per chunk via `LIMIT chunkSize,1`.
- [inferred] A row-constructor comparison keeps parameter count linear (`2N+1` range parameters per split), while expanded OR predicates require repeated prefixes and grow quadratically; start with row constructors for the existing MySQL-only scope.
- [inferred] MySQL-compatible engines must still pass `EXPLAIN` validation because optimizer behavior can differ. MySQL documents that row constructors use indexes best when their columns cover the composite-index prefix: https://dev.mysql.com/doc/refman/8.0/en/row-constructor-optimization.html.

## 6. Proposed Changes

### 6.1 Configuration and canonical model

1. **`JdbcConnectorOptions` — add `SCAN_PARTITION_COLUMNS`.**
   - Add `scan.partition.columns` as a comma-separated ordered field-name string, documented with `tenant_id,order_id`.
   - Keep `scan.partition.column` unchanged for fixed-range and backward-compatible one-column dynamic jobs.
   - Do not silently merge singular and plural options.

2. **`JdbcReadOptions` — canonical ordered list.**
   - Add immutable `List<String> partitionColumnNames` and `getPartitionColumnNames()`.
   - Keep `setPartitionColumnName`/`getPartitionColumnName` for compatibility; the setter creates a singleton list and the getter is valid only for a singleton.
   - Add `setPartitionColumnNames(List<String>)` with a defensive copy.
   - Include ordered names in constructor, `equals`, and any future `hashCode` implementation.

3. **`JdbcDynamicTableFactory` — parse and validate once.**
   - Add `resolvePartitionColumns(config, strategy)` as the single option-resolution path.
   - Dynamic mode accepts exactly one of singular or plural; fixed-range rejects plural.
   - Trim plural values and reject empty entries, duplicate names, and an empty list.
   - Validate every name against the physical schema and reject every nullable field with a column-specific message.
   - Preserve the existing MySQL dialect restriction and all chunk/retry/timeout validation.
   - Do not hard-require a declared Flink primary key: existing dynamic jobs permit non-unique columns. Document that exact PK/UNIQUE order is the production recommendation.

### 6.2 Shared tuple SQL and parameter layout

4. **New package-private `JdbcDynamicKeysetSqlBuilder` in the table source package.**
   - Quote the ordered identifiers once through `JdbcDialect`.
   - Render tuple expressions `(k1, k2, ...)` and placeholders `(?, ?, ...)`.
   - Render:
     - first key query: `SELECT keys FROM table [WHERE filters] ORDER BY keys LIMIT 1`;
     - boundary query: `... WHERE filters AND (keys) >= (cursor) ORDER BY keys LIMIT chunkSize,1`;
     - next-distinct query: `... WHERE filters AND (keys) > (cursor) ORDER BY keys LIMIT 1`;
     - data range predicate: `(keys) >= (lower) AND (? IS NULL OR (keys) < (upper))`.
   - For one key, emit the current scalar SQL shape exactly, preserving `[start,end,end]` compatibility.
   - Provide parameter-layout methods so discovery and data code do not hand-assemble placeholder arrays independently.
   - Keep a renderer seam so an expanded lexicographic predicate can be added for a MySQL-compatible engine that fails the row-constructor `EXPLAIN` gate; do not add a public tuning option until evidence requires it.

5. **New package-private immutable `JdbcKeyTuple`.**
   - Store a non-empty ordered `Serializable[]` with defensive copies, tuple equality/hash, and column-count validation.
   - Reject null components and non-serializable JDBC objects with the offending configured column in the error.
   - Keep this object discovery-local; flatten scalar components before returning `Serializable[][]`, so `JdbcParameterValuesInputSplit` serialization remains unchanged.

### 6.3 Discovery and scan execution

6. **`JdbcDynamicKeysetParametersProvider` — generalize scalar cursor to tuple cursor.**
   - Replace `quotedPartitionColumn` with the ordered builder/list.
   - Replace aggregate `MIN(key)` with `ORDER BY keys LIMIT 1`; this works for mixed-type tuples and preserves database collation/order.
   - Change query result extraction to read all key columns into `JdbcKeyTuple`; zero rows means no key/tail instead of a mandatory aggregate row.
   - Bind cursor components in builder-defined order and read the same number/order of result columns.
   - Preserve duplicate-tuple fallback: if offset boundary equals the cursor, query the next strictly greater tuple; throw a defensive non-progress error if it still does not advance.
   - Flatten data parameters as `[lower components, upper-first sentinel, upper components, filter parameters]`. For one column this remains `[start,end,end,filters...]`.
   - Preserve maximum-split enforcement, progress logging, reusable statements, transient classification, reconnect, backoff, and total/query timeouts.
   - Include ordered key names in equality/hash semantics and diagnostic logs.

7. **`JdbcDynamicTableSource` — consume the shared builder.**
   - Resolve the ordered key list from `JdbcReadOptions`.
   - Construct the provider and data range predicate from the same `JdbcDynamicKeysetSqlBuilder` instance/configuration.
   - Preserve limit fallback, projection behavior, pushed-filter ordering, fetch-size handling, and fixed-range code unchanged.

8. **`JdbcRowDataInputFormat` — preserve existing scalar binder.**
   - Do not teach the input format about tuples; it should continue binding flattened scalar values.
   - Preserve the current uncommitted `BigInteger -> BigDecimal` binding and its regression test.
   - Add binder coverage only if a newly supported composite-key JDBC type produces an unsupported Java object.

### 6.4 Documentation and operational contract

9. Update English and Chinese JDBC docs with:
   - single-column compatibility and plural configuration examples;
   - exact ordered composite `PRIMARY KEY`/`UNIQUE NOT NULL` index recommendation;
   - all key columns must be non-null and use the same order/collation across discovery and scans;
   - `WHERE` filters participate in both boundary and data queries;
   - chunk size is a target, not a hard maximum for non-unique tuples;
   - no cross-connection snapshot, no protection from concurrent key updates/inserts behind the cursor, and split retries can repeat rows;
   - MySQL is supported by automated tests; TiDB and OceanBase MySQL mode require explicit compatibility and `EXPLAIN` validation before being advertised as supported.

Recommended SQL DDL:

```sql
WITH (
  'connector' = 'jdbc',
  'scan.partition.strategy' = 'dynamic-keyset',
  'scan.partition.columns' = 'tenant_id,order_id',
  'scan.partition.chunk-size' = '50000'
)
```

Required index shape:

```sql
PRIMARY KEY (tenant_id, order_id)
-- or UNIQUE NOT NULL INDEX in exactly the configured order
```

## 7. Implementation Sequence

1. Add plural option resolution and ordered-list storage, retaining singular builder/getter compatibility; land factory unit tests with this step.
2. Add `JdbcKeyTuple` and `JdbcDynamicKeysetSqlBuilder` with focused SQL/placeholder-layout tests for one, two, and three columns.
3. Refactor provider query extraction/binding from scalar to tuple while keeping single-column tests byte-for-byte equivalent in expected parameter matrices.
4. Add composite provider tests for sparse keys, filters, mixed types, duplicate tuples, empty/tail chunks, max splits, transient retry, and reconnect-at-current-tuple.
5. Wire the ordered list and shared SQL builder into `JdbcDynamicTableSource`; leave fixed-range and limit fallback untouched.
6. Add MySQL Testcontainers end-to-end coverage with a true composite primary key and verify every row appears exactly once.
7. Add `EXPLAIN` assertions/manual acceptance evidence for MySQL and run staging probes against TiDB/OceanBase before enabling their support claim.
8. Update bilingual documentation and configuration tables after behavior and syntax are fixed.
9. Run formatting, focused core tests, MySQL tests, and the affected module build; review the final diff to ensure the existing BigInteger changes remain intact.

## 8. Test Strategy

### Core unit tests

`JdbcDynamicTableFactoryTest` / `MySqlDynamicKeysetTableFactoryTest`:

- singular dynamic column still creates the same source;
- plural two-column option creates a source;
- singular plus plural is rejected;
- plural with empty/duplicate/missing/nullable fields is rejected;
- plural is rejected for fixed-range;
- invalid chunk/retry/timeout validation is unchanged.

New `JdbcDynamicKeysetSqlBuilderTest`:

- one-column SQL and parameter order match current behavior;
- two/three-column first/boundary/distinct/data SQL quote every identifier;
- lower, bounded-upper, unbounded-upper, and filter parameter orders match placeholders;
- table/column identifiers are never concatenated unquoted.

`JdbcDynamicKeysetParametersProviderTest`:

- sparse `(tenant_id BIGINT, order_id BIGINT)` values form expected tuple ranges;
- string + unsigned numeric tuple preserves JDBC ordering and `BigInteger` values;
- pushed filter affects first key and every subsequent boundary;
- duplicate full tuples produce an oversized but complete split without looping;
- no matching rows returns zero splits;
- final partial chunk has a null upper tuple;
- retry/reconnect resumes the current tuple and does not restart discovery;
- max-splits and total timeout still fail with actionable messages.

### MySQL integration

Extend `MySqlDynamicTableSourceITCase` or add a dedicated composite-key IT case:

- create a table with `PRIMARY KEY (tenant_id, order_id)` and highly sparse IDs;
- use parallelism > 1 and chunk size 2;
- assert exact row set, no duplicates, and filtered exact row set;
- include repeated `tenant_id` values so single-column splitting would skew;
- include a terminal maximum tuple and a final partial chunk;
- inspect `EXPLAIN` to confirm the composite index is selected and no full table scan/filesort appears for the tested query shape.

### Verification commands

```bash
mvn -q -pl flink-connector-jdbc-core -DskipITs -Dcheckstyle.skip -Drat.skip \
  -Dtest=JdbcDynamicKeysetParametersProviderTest,JdbcDynamicKeysetSqlBuilderTest,JdbcDynamicTableFactoryTest,JdbcRowDataInputFormatTest test

mvn -q -pl flink-connector-jdbc-mysql -am \
  -Dtest=MySqlDynamicKeysetTableFactoryTest,MySqlDynamicTableSourceITCase test

mvn -q -pl flink-connector-jdbc-core,flink-connector-jdbc-mysql -am spotless:check checkstyle:check test
```

The first existing focused baseline command was executed successfully during planning. MySQL integration requires Docker/Testcontainers.

## 9. Risk and Impact Analysis

- **Range correctness — high:** tuple comparison and `ORDER BY` must use identical ordered columns and database collation. Centralize them in one builder and assert placeholder order.
- **Optimizer behavior — high:** MySQL-compatible syntax does not guarantee identical access plans. Gate TiDB/OceanBase support on actual `EXPLAIN` and load testing.
- **Memory — medium:** all split parameter rows are materialized before execution. Row-constructor predicates keep each row at `2N+1+filterParams`; very large `max-splits` and wide keys still increase JobManager/client memory.
- **Database load — medium:** discovery reads roughly one composite-index pass plus the data scan; missing/misordered indexes can turn every boundary query into sort/scan pressure.
- **Concurrent writes — high but pre-existing:** JDBC parallel reads do not provide one global snapshot. Inserts/updates/deletes during discovery/scan can be missed, repeated, or observed inconsistently.
- **Retry semantics — medium:** a task failure reruns its full split; downstream must tolerate duplicates or provide idempotence/transactional commit behavior.
- **Type binding — medium:** returned JDBC scalar classes must be serializable and supported by `JdbcRowDataInputFormat`; preserve and test unsigned `BIGINT` handling.
- **API compatibility — medium:** never reinterpret comma characters inside the existing singular option. Introduce a distinct plural option and reject simultaneous use.
- **Projection/filter interaction — low:** partition fields need not be selected, but must remain valid physical source fields. Existing filter parameter ordering must remain unchanged.
- **Limit pushdown — low:** current global-limit branch intentionally disables parallel dynamic splits and must remain unchanged.

Observability additions:

- log ordered partition columns, chunk size, discovered split count, elapsed discovery time, and periodic progress;
- never log credentials or full bound values by default;
- include query kind and column list in non-progress/type errors;
- optionally expose discovery query count and elapsed time as metrics in a later Source API migration, not in this change.

## 10. Files Expected to Change

| File | Symbols | Reason |
| ---- | ------- | ------ |
| `flink-connector-jdbc-core/.../table/JdbcConnectorOptions.java` | `SCAN_PARTITION_COLUMNS` | Add explicit ordered composite configuration |
| `flink-connector-jdbc-core/.../internal/options/JdbcReadOptions.java` | fields, builder, equality | Carry immutable ordered columns |
| `flink-connector-jdbc-core/.../table/JdbcDynamicTableFactory.java` | option resolution and validation | Enforce exclusive options and validate every key |
| `flink-connector-jdbc-core/.../table/source/JdbcDynamicKeysetSqlBuilder.java` | new class | Own tuple SQL and placeholder layouts |
| `flink-connector-jdbc-core/.../table/source/JdbcKeyTuple.java` | new class | Own discovery-local tuple values |
| `flink-connector-jdbc-core/.../table/source/JdbcDynamicKeysetParametersProvider.java` | discovery loop/executor/query builders | Discover and flatten tuple boundaries |
| `flink-connector-jdbc-core/.../table/source/JdbcDynamicTableSource.java` | `getScanRuntimeProvider` | Generate composite range scans from shared builder |
| `flink-connector-jdbc-core/.../source/JdbcDynamicKeysetParametersProviderTest.java` | provider tests | Tuple correctness, failures, retries |
| `flink-connector-jdbc-core/.../source/JdbcDynamicKeysetSqlBuilderTest.java` | new tests | SQL and parameter contract |
| `flink-connector-jdbc-core/.../table/JdbcDynamicTableFactoryTest.java` | validation tests | Core option compatibility |
| `flink-connector-jdbc-mysql/.../table/MySqlDynamicKeysetTableFactoryTest.java` | MySQL factory tests | Nullable/missing/multi-column validation |
| `flink-connector-jdbc-mysql/.../table/MySqlDynamicTableSourceITCase.java` | integration scenario | Real composite index and end-to-end scan |
| `docs/content/docs/connectors/table/jdbc.md` | option table/guide | English contract |
| `docs/content.zh/docs/connectors/table/jdbc.md` | option table/guide | Chinese contract |

`JdbcRowDataInputFormat.java` and `JdbcRowDataInputFormatTest.java` are not expected to need new tuple logic; their existing dirty BigInteger changes are protected dependencies.

## 11. Reusable Implementation Context

```yaml
implementation_context:
  task_summary: "Extend dynamic-keyset from scalar to ordered composite sparse JDBC partitioning while preserving single-column behavior."
  acceptance_criteria:
    - "Existing scan.partition.column jobs retain current SQL/parameter behavior."
    - "Composite ranges are exhaustive, non-overlapping and ordered by database tuple semantics."
    - "Filters participate in discovery and scan queries."
    - "Every key is present and NOT NULL; MySQL end-to-end tests pass."
  evidence_provenance:
    schema_version: 2
    head_commit: "10849fb3cd9fc096288c9c4ea1a613cf67a792f3"
    generated_plan_path: "docs/plans/2026-11-02-gitnexus-plan-composite-keyset-partitioning.md"
    global_dirty_digest:
      algorithm: "sha256"
      canonicalization: "gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records"
      value: "d573fda36db8498ad2065a60cebc7c3315a967760004730b981dc5d91295a945"
    cited_path_manifest:
      - {path: "docs/content.zh/docs/connectors/table/jdbc.md", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:4854dd476d072640af6f37ee884b57668f21d8562c6d9cb91e11199283381457", index_digest: "sha256:4854dd476d072640af6f37ee884b57668f21d8562c6d9cb91e11199283381457", worktree_digest: "sha256:4854dd476d072640af6f37ee884b57668f21d8562c6d9cb91e11199283381457", untracked_digest: absent}
      - {path: "docs/content/docs/connectors/table/jdbc.md", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:38b28cffa9626ad90b7b77e26a3736483a19cf09ab8574049bb22ff525c86c9c", index_digest: "sha256:38b28cffa9626ad90b7b77e26a3736483a19cf09ab8574049bb22ff525c86c9c", worktree_digest: "sha256:38b28cffa9626ad90b7b77e26a3736483a19cf09ab8574049bb22ff525c86c9c", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/pom.xml", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:929c5de6c1053b08ce57a28f5b5b1f33bcf0b88cde1054c27d91fdce49e9e9c1", index_digest: "sha256:929c5de6c1053b08ce57a28f5b5b1f33bcf0b88cde1054c27d91fdce49e9e9c1", worktree_digest: "sha256:929c5de6c1053b08ce57a28f5b5b1f33bcf0b88cde1054c27d91fdce49e9e9c1", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/JdbcConnectorOptions.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:d94a6a891f84320af8d8250d77f734d9fe7783083bbabfaf77cf9e8ff8cd5d99", index_digest: "sha256:d94a6a891f84320af8d8250d77f734d9fe7783083bbabfaf77cf9e8ff8cd5d99", worktree_digest: "sha256:d94a6a891f84320af8d8250d77f734d9fe7783083bbabfaf77cf9e8ff8cd5d99", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/JdbcDynamicTableFactory.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:eb1b8425a61d16e1fdc4d02ba33e2e0f09322b27f94c4c6b8169c18f43ee4cb9", index_digest: "sha256:eb1b8425a61d16e1fdc4d02ba33e2e0f09322b27f94c4c6b8169c18f43ee4cb9", worktree_digest: "sha256:eb1b8425a61d16e1fdc4d02ba33e2e0f09322b27f94c4c6b8169c18f43ee4cb9", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicKeysetParametersProvider.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:013f29307df49bc49a8f089e450261c5fbb6ccd4eb576fe896fd8b09fbf05759", index_digest: "sha256:013f29307df49bc49a8f089e450261c5fbb6ccd4eb576fe896fd8b09fbf05759", worktree_digest: "sha256:013f29307df49bc49a8f089e450261c5fbb6ccd4eb576fe896fd8b09fbf05759", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicTableSource.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:9cbcc2ecaea0e94e4be29eccbe3a627d967656a155450cacee59a2993bdf2bc6", index_digest: "sha256:9cbcc2ecaea0e94e4be29eccbe3a627d967656a155450cacee59a2993bdf2bc6", worktree_digest: "sha256:9cbcc2ecaea0e94e4be29eccbe3a627d967656a155450cacee59a2993bdf2bc6", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcParameterValuesInputSplit.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:4c75e1ab2b3a7a0a0adfad290fe7456322701f5a5b2ce3d087c1187fee6a596b", index_digest: "sha256:4c75e1ab2b3a7a0a0adfad290fe7456322701f5a5b2ce3d087c1187fee6a596b", worktree_digest: "sha256:4c75e1ab2b3a7a0a0adfad290fe7456322701f5a5b2ce3d087c1187fee6a596b", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcRowDataInputFormat.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: unstaged, rename_from: null, rename_to: null, head_digest: "sha256:cca688813f5e7631c3c12d3c95ebbe2825ac3378f9ebce92ef67d411ca3072cb", index_digest: "sha256:cca688813f5e7631c3c12d3c95ebbe2825ac3378f9ebce92ef67d411ca3072cb", worktree_digest: "sha256:5efa826fb00b929b4cfdd84f2872b7bd8a2bdc3100a28b5158e7d46a240c415a", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/internal/options/JdbcReadOptions.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:643a56a9a98068a734de39cb5edc1b1630e7bc0c0d1cca85986cc598da56608a", index_digest: "sha256:643a56a9a98068a734de39cb5edc1b1630e7bc0c0d1cca85986cc598da56608a", worktree_digest: "sha256:643a56a9a98068a734de39cb5edc1b1630e7bc0c0d1cca85986cc598da56608a", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/test/java/org/apache/flink/connector/jdbc/core/table/JdbcDynamicTableFactoryTest.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:f707a70fcc1256632d5fc459269125f35ce4e447c43bf164258be0e31fe6dfba", index_digest: "sha256:f707a70fcc1256632d5fc459269125f35ce4e447c43bf164258be0e31fe6dfba", worktree_digest: "sha256:f707a70fcc1256632d5fc459269125f35ce4e447c43bf164258be0e31fe6dfba", untracked_digest: absent}
      - {path: "flink-connector-jdbc-core/src/test/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicKeysetParametersProviderTest.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:c7e2ca831e511965ac9cc03a30d6031c0f5f90d5bbd10ed642ab34854e0d0b1f", index_digest: "sha256:c7e2ca831e511965ac9cc03a30d6031c0f5f90d5bbd10ed642ab34854e0d0b1f", worktree_digest: "sha256:c7e2ca831e511965ac9cc03a30d6031c0f5f90d5bbd10ed642ab34854e0d0b1f", untracked_digest: absent}
      - {path: "flink-connector-jdbc-mysql/pom.xml", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:c429197a6a3183cb70391eb8c0b6a5b58c9085f13a56c428c30ac24a0a9f37d9", index_digest: "sha256:c429197a6a3183cb70391eb8c0b6a5b58c9085f13a56c428c30ac24a0a9f37d9", worktree_digest: "sha256:c429197a6a3183cb70391eb8c0b6a5b58c9085f13a56c428c30ac24a0a9f37d9", untracked_digest: absent}
      - {path: "flink-connector-jdbc-mysql/src/test/java/org/apache/flink/connector/jdbc/mysql/table/MySqlDynamicKeysetTableFactoryTest.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:fad64f1802598d1c6c5a32f227b890a2ccb2a9f7c23ba03b42bf548a505c8333", index_digest: "sha256:fad64f1802598d1c6c5a32f227b890a2ccb2a9f7c23ba03b42bf548a505c8333", worktree_digest: "sha256:fad64f1802598d1c6c5a32f227b890a2ccb2a9f7c23ba03b42bf548a505c8333", untracked_digest: absent}
      - {path: "flink-connector-jdbc-mysql/src/test/java/org/apache/flink/connector/jdbc/mysql/table/MySqlDynamicTableSourceITCase.java", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:8d9110e01ea81acaa206ce80f275dab624bcfbe9e230417d7972808ab18aafac", index_digest: "sha256:8d9110e01ea81acaa206ce80f275dab624bcfbe9e230417d7972808ab18aafac", worktree_digest: "sha256:8d9110e01ea81acaa206ce80f275dab624bcfbe9e230417d7972808ab18aafac", untracked_digest: absent}
      - {path: "pom.xml", object_kind: {head: regular, index: regular, worktree: regular, untracked: absent}, state: clean, rename_from: null, rename_to: null, head_digest: "sha256:a06125b17619f235749f7cbcdcf45d384c4e9ef6365f1bda60b87733ed21a2d8", index_digest: "sha256:a06125b17619f235749f7cbcdcf45d384c4e9ef6365f1bda60b87733ed21a2d8", worktree_digest: "sha256:a06125b17619f235749f7cbcdcf45d384c4e9ef6365f1bda60b87733ed21a2d8", untracked_digest: absent}
  primary_symbols:
    - {symbol: "JdbcDynamicKeysetParametersProvider.discoverParameterValues", file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicKeysetParametersProvider.java", lines: "112-174", role: "Discover ordered sparse boundaries and emit split parameters"}
    - {symbol: "JdbcDynamicTableSource.getScanRuntimeProvider", file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicTableSource.java", lines: "130-219", role: "Build shared scan SQL and provider"}
    - {symbol: "JdbcDynamicTableFactory", file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/JdbcDynamicTableFactory.java", lines: "123-169,201-229,347-414", role: "Resolve and validate configuration"}
  related_symbols:
    - {symbol: "JdbcReadOptions", relationship: "configuration DTO", relevance: "Carries ordered keys into runtime"}
    - {symbol: "JdbcRowDataInputFormat.createInputSplits/open", relationship: "consumes provider output", relevance: "Serializes and binds flattened scalar split parameters"}
    - {symbol: "JdbcParameterValuesInputSplit", relationship: "split transport", relevance: "Must remain scalar-array compatible"}
  execution_path:
    - "Factory resolves one ordered key list and validates every field."
    - "Source and provider share one tuple SQL/parameter-layout builder."
    - "Provider discovers first key and offset boundaries using database ordering."
    - "Provider flattens [lower, upper sentinel, upper, filters] into serializable split rows."
    - "InputFormat creates one split per row and tasks bind scalars into the common SQL template."
  pdg_constraints:
    - {description: "PDG unavailable; source shows cursor advances only after successful query", affected_statements: ["JdbcDynamicKeysetParametersProvider.java:129-146"], implementation_consequence: "Reconnect/retry must resume the same tuple"}
    - {description: "Data SQL places range placeholders before pushed-filter placeholders", affected_statements: ["JdbcDynamicTableSource.java:171-205"], implementation_consequence: "Flattening order must remain range then filter"}
  architectural_patterns:
    - {pattern: "Provider computes all serializable parameter rows before split creation", example_location: "JdbcRowDataInputFormat.java:createInputSplits", usage_guidance: "Keep tuple objects discovery-local and flatten before transport"}
    - {pattern: "Discovery reuses prepared statements and reconnects only on connection failures", example_location: "JdbcDynamicKeysetParametersProvider.java:DiscoveryQueryExecutor", usage_guidance: "Generalize result shape without changing failure policy"}
  files_to_modify:
    - {file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/JdbcConnectorOptions.java", symbols: ["SCAN_PARTITION_COLUMNS"], intended_change: "Add ordered plural option"}
    - {file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/internal/options/JdbcReadOptions.java", symbols: ["Builder", "partitionColumnNames"], intended_change: "Carry immutable ordered keys"}
    - {file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/JdbcDynamicTableFactory.java", symbols: ["validateDynamicPartitionColumn", "getJdbcReadOptions", "validateConfigOptions"], intended_change: "Resolve and validate every key"}
    - {file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicKeysetSqlBuilder.java", symbols: ["new class"], intended_change: "Centralize tuple SQL and placeholder order"}
    - {file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcKeyTuple.java", symbols: ["new class"], intended_change: "Represent discovery-local ordered boundary values"}
    - {file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicKeysetParametersProvider.java", symbols: ["discoverParameterValues", "DiscoveryQueryExecutor"], intended_change: "Discover composite tuple boundaries"}
    - {file: "flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicTableSource.java", symbols: ["getScanRuntimeProvider"], intended_change: "Use shared composite range SQL"}
  tests:
    - {file: "flink-connector-jdbc-core/src/test/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicKeysetSqlBuilderTest.java", scenarios: ["one-column compatibility", "2/3-column SQL", "bounded/unbounded parameter layout"]}
    - {file: "flink-connector-jdbc-core/src/test/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicKeysetParametersProviderTest.java", scenarios: ["sparse tuples", "filters", "duplicate tuples", "retry/reconnect", "tail and empty"]}
    - {file: "flink-connector-jdbc-core/src/test/java/org/apache/flink/connector/jdbc/core/table/JdbcDynamicTableFactoryTest.java", scenarios: ["option exclusivity", "empty/duplicate/missing/nullable keys", "fixed-range rejection"]}
    - {file: "flink-connector-jdbc-mysql/src/test/java/org/apache/flink/connector/jdbc/mysql/table/MySqlDynamicTableSourceITCase.java", scenarios: ["true composite PK end-to-end", "parallel filtered scan", "EXPLAIN composite-index usage"]}
  verification_commands:
    - "mvn -q -pl flink-connector-jdbc-core -DskipITs -Dcheckstyle.skip -Drat.skip -Dtest=JdbcDynamicKeysetParametersProviderTest,JdbcDynamicKeysetSqlBuilderTest,JdbcDynamicTableFactoryTest,JdbcRowDataInputFormatTest test"
    - "mvn -q -pl flink-connector-jdbc-mysql -am -Dtest=MySqlDynamicKeysetTableFactoryTest,MySqlDynamicTableSourceITCase test"
    - "mvn -q -pl flink-connector-jdbc-core,flink-connector-jdbc-mysql -am spotless:check checkstyle:check test"
  risks:
    - "Tuple ordering, placeholder ordering, and index order must match exactly."
    - "MySQL-compatible engines may optimize row constructors differently."
    - "All split rows are materialized, so wide tuples and high split counts consume more coordinator memory."
    - "Parallel JDBC scans are not a global consistent snapshot and split retries can duplicate rows."
  assumptions:
    - "Verify MySQL row-constructor range access with Testcontainers EXPLAIN before implementation completion."
    - "Verify TiDB and OceanBase MySQL mode syntax and EXPLAIN on target versions before advertising support."
    - "Recheck the two dirty BigInteger files and preserve their worktree changes before editing overlapping binding code."
  open_questions:
    - "Which exact TiDB and OceanBase versions must become supported targets rather than validated deployments?"
    - "Should a future option select expanded lexicographic predicates if a compatible engine cannot optimize row constructors?"
  avoid:
    - "Do not repeat full repository discovery."
    - "Do not overload scan.partition.column with comma parsing."
    - "Do not generate independent per-column BETWEEN predicates."
    - "Do not compare tuple values in Java to define database order."
    - "Do not replace the current BigInteger parameter binding changes."
    - "Do not claim CDC-grade consistency for parallel JDBC snapshots."
```

## 12. Assumptions and Open Questions

### Assumptions to verify during implementation

1. [assumed] Target MySQL versions optimize the full-prefix row-constructor range predicates with the intended composite index; verify via Testcontainers `EXPLAIN`.
2. [assumed] Target TiDB and OceanBase MySQL-mode versions implement compatible row comparison and parameter typing; verify against the actual deployed versions before support is documented.
3. [assumed] Composite-key fields returned by supported drivers map to scalar Java classes already handled by `JdbcRowDataInputFormat`; add focused binders only when a real unsupported class is observed.
4. [assumed] Creating all splits eagerly remains acceptable for the expected `max-splits`; record heap usage for wide keys during load testing.

### Open questions

1. Exact TiDB/OceanBase production versions and whether they are formal support targets or best-effort compatible deployments.
2. Whether operational policy should require configured fields to exactly equal a declared Flink primary key, or continue allowing a documented non-unique tuple with possible oversized chunks.
3. Whether a later release should add an expanded-predicate renderer option; defer unless `EXPLAIN` demonstrates a real target-engine need.

### Explicitly deferred

- Migration from legacy `InputFormatSourceFunction` to FLIP-27 Source/SplitEnumerator with incremental split discovery and checkpointed enumeration state.
- CDC/binlog correction for concurrent writes.
- Automatic remote index introspection or DDL creation.
- New discovery metrics beyond existing logs.

## 13. Definition of Done

- Both singular and plural dynamic-keyset configurations are documented and validated.
- Single-column provider tests retain current expected parameter matrices.
- Composite provider tests prove sparse, filtered, duplicate, empty, tail, retry, reconnect, timeout, and max-split behavior.
- MySQL Testcontainers reads a sparse composite-primary-key table in parallel with exact-once row-set coverage at the source result level.
- `EXPLAIN` confirms expected composite-index access for first, boundary, next-distinct, and data queries.
- Core/MySQL focused tests, formatting, checkstyle, and affected module tests pass.
- Current uncommitted BigInteger support remains present and tested.
- English/Chinese docs clearly state index requirements and non-CDC consistency/retry limits.
- TiDB/OceanBase are not labeled supported until their exact deployed versions pass syntax, correctness, `EXPLAIN`, timeout, and load validation.
