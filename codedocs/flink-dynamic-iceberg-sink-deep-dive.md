<!-- Local working notes for Claude / engineers. Not for upstream contribution. -->

# Flink Dynamic Iceberg Sink — Deep Dive

A code-level study of the **Flink Dynamic Iceberg Sink** (`org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink`),
the Flink connector that lets a *single* sink write to *any number* of Iceberg tables, creating
and evolving those tables (schema + partition spec) at runtime based on per-record routing
metadata — with no Flink job restart.

> Scope: this doc is written against the source in
> `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/`. The same package
> exists identically under `flink/v2.0/...` and `flink/v2.1/...` (55 files each). All class names,
> method signatures, and `file:line` citations below were read directly from that source tree.
> The feature is annotated `@Experimental` (`DynamicIcebergSink.java:66`).

Official docs: <https://iceberg.apache.org/docs/nightly/flink-writes/#flink-dynamic-iceberg-sink>
(rendered from `docs/docs/flink-writes.md`). Apache Flink blog walkthrough:
<https://flink.apache.org/2025/10/14/from-stream-to-lakehouse-kafka-ingestion-with-the-flink-dynamic-iceberg-sink/>.
Design/proposal issue: <https://github.com/apache/iceberg/issues/11536>.

---

## 1. Why this feature exists

### The classic `IcebergSink` / `FlinkSink` is 1:1 and static

The standard Flink → Iceberg sinks (`org.apache.iceberg.flink.sink.IcebergSink`,
and the older `FlinkSink`) are bound at **job-build time** to exactly one table whose **schema**
and **partition spec** are fixed when the job graph is constructed:

```java
FlinkSink.forRowData(dataStream)
    .table(table)                 // ONE concrete table, resolved now
    .tableLoader(tableLoader)
    .append();
```

Everything downstream — the `RowType` the serializers expect, the writer's `TaskWriterFactory`,
the partitioner — is specialized for that one `(table, schema, spec)`. Consequences:

- **One sink per table.** Writing N tables means building N sinks (and often N Kafka sources / N
  branches of the job graph). For CDC fan-out from a single topic carrying many entity types, or
  multi-tenant ingestion, this explodes operationally.
- **Schema is frozen at deploy.** If the upstream schema evolves (a new Avro field appears), the
  job must be stopped, the table altered out-of-band, and the job redeployed with the new schema.
- **The target table set must be known up front.** You cannot discover "topic → table" mappings at
  runtime, and you cannot auto-create tables you haven't seen yet.

### What the dynamic sink changes

`DynamicIcebergSink`'s own Javadoc (`DynamicIcebergSink.java:57-65`) states it supports:

> - Writing to any number of tables (No more 1:1 sink/topic relationship).
> - Creating and updating tables based on the user-supplied routing.
> - Updating the schema and partition spec of tables based on the user-supplied specification.

The pivot is that the **per-record metadata** — which table, which branch, what schema, what
partition spec, what distribution/parallelism, whether upsert — travels *with each record*, inside
a `DynamicRecord` produced by a user-supplied `DynamicRecordGenerator`. The sink resolves that
metadata against the catalog (creating/evolving tables as needed) and routes the row to the right
writer, all while the job keeps running. Because Iceberg schema evolution is a **metadata-only**
operation, adding a column never rewrites existing data files.

Stated advantages (docs "Flink Dynamic Iceberg Sink" section):

1. **Writing to any number of tables** — one sink dynamically routes records to many tables.
2. **Dynamic table creation and updates** — tables created/updated from user routing logic.
3. **Dynamic schema and partition evolution** — schemas and specs update during streaming.

---

## 2. When to use it (and when not)

**Good fits**

- **CDC / event fan-out**: one Kafka topic (or a small set) carries records for many tables; route
  by an embedded table name / entity type. The Flink blog's reference pipeline reads *all* topics
  with one source and writes them with one `DynamicIcebergSink`.
- **Multi-tenant ingestion**: per-tenant tables created on demand.
- **Evolving upstream schemas**: producers add columns / widen types and you want the lake to track
  them without a redeploy (e.g. Avro schema-registry driven).
- **Unknown table set at deploy time**: tables are created the first time a record for them arrives.

**Be careful / not ideal**

- The API is `@Experimental` — signatures can change between releases.
- **Per-record overhead**: every record is checked against caches and (on the slow path) the
  catalog. Mitigated heavily by the metadata + input-schema caches (§7.3), but it is more work than
  the static sink's hot path. Reusing the *same* `Schema` instance across records maximizes cache
  hits (docs "Caching").
- **Catalog write pressure / backpressure** when many *new* schemas/tables appear at once. The
  default (non-immediate) update path funnels updates through a table-keyed operator to serialize
  them; `immediateTableUpdate(true)` does them inline (lower latency, more catalog load — see §5).
- **Not for table-format upgrades**: the sink must not run during a V2→V3 upgrade (docs "Notes";
  enforced defensively at commit, `DynamicCommitter.java:219-231`).
- **No RANGE distribution** (falls back to HASH), **no column rename** (name-based comparison).

---

## 3. Core concepts & data model

### 3.1 `DynamicRecord` (public, user-facing)

`DynamicRecord.java:30-144`. This is what your generator emits; it pairs a `RowData` with the
Iceberg metadata describing where/how to write it.

| Field | Type | Meaning |
|---|---|---|
| `tableIdentifier` | `TableIdentifier` | Target table. |
| `branch` | `String` | Target branch (e.g. `main`). |
| `schema` | `Schema` | The schema of *this* record's row. |
| `rowData` | `RowData` | The actual Flink row. |
| `partitionSpec` | `PartitionSpec` | Desired partition spec for the target table. |
| `distributionMode` | `DistributionMode` | `HASH`, `NONE`, or `null` (see §6). |
| `writeParallelism` | `int` | Max parallel writers for this `(table/branch/schema/spec)`. Capped by sink parallelism; `Integer.MAX_VALUE` ⇒ use max available (`DynamicRecord.java:51-54`). |
| `upsertMode` | `boolean` | Overrides the table's `write.upsert.enabled` (setter only). |
| `equalityFields` | `Set<String>` (nullable) | Equality field **names** for upsert/equality deletes (setter only). |

The single public constructor (`DynamicRecord.java:56-71`) takes the first seven; `upsertMode`
(default `false`) and `equalityFields` (default `null`) are set via `setUpsertMode` /
`setEqualityFields`.

### 3.2 `DynamicRecordGenerator` (the user's routing function)

`DynamicRecordGenerator.java` (entire interface):

```java
public interface DynamicRecordGenerator<T> extends Serializable {
  default void open(OpenContext openContext) throws Exception {}

  /** Yields zero, one, or multiple DynamicRecords using the Collector. */
  void generate(T inputRecord, Collector<DynamicRecord> out) throws Exception;
}
```

It is a `Collector`-based emitter (0..N records per input), *not* a `convert→return` mapper. (The
nightly docs show an illustrative `DynamicRecord generate(RowData)` form — that does not match the
real interface; the real contract is the `Collector` form above, as in the docs' first lambda
example and the tests.) There is also an abstract convenience base
`DynamicTableRecordGenerator extends DynamicRecordGenerator<RowData>` holding a `RowType`.

### 3.3 `DynamicRecordInternal` (internal pipeline element)

`DynamicRecordInternal.java:33-40`. After the generator output is resolved, the processor converts
each public `DynamicRecord` into this `@Internal` type, which is what actually flows through the
writer/committer.

```java
private String tableName;           // TableIdentifier.toString()
private String branch;
private Schema schema;              // the RESOLVED table schema
private PartitionSpec spec;        // the RESOLVED spec
private int writerKey;             // precomputed routing key (from HashKeyGenerator)
private RowData rowData;           // row already converted to the target schema
private boolean upsertMode;
private Set<Integer> equalityFieldIds;  // resolved from names → field ids
```

Differences from the public `DynamicRecord`:

| Aspect | `DynamicRecord` (public) | `DynamicRecordInternal` (internal) |
|---|---|---|
| Table identity | `TableIdentifier` | `String tableName` |
| Equality fields | `Set<String>` (names) | `Set<Integer>` (resolved ids) |
| `distributionMode`, `writeParallelism` | present | **absent** — already consumed to compute `writerKey` |
| Routing | none | `int writerKey` |
| `rowData` / `schema` | as supplied | converted/resolved to the target table |

So `DynamicRecordProcessor` (and `DynamicTableUpdateOperator`) is the exact boundary that turns the
public record into the internal one.

### 3.4 `WriteTarget` and `TableKey` (identity downstream)

- `WriteTarget` (`WriteTarget.java`): the *writer-output* identity =
  `{tableName, branch (defaults to MAIN_BRANCH), schemaId, specId, upsertMode, equalityFields:Set<Integer>}`.
  It is the map key for `DynamicWriter`'s per-target writers.
- `TableKey` (`TableKey.java`): the *commit-grouping* identity = `{tableName, branch}`. Used by the
  aggregator and committer to group write results into one snapshot per table/branch.

---

## 4. Architecture & operator topology

`DynamicIcebergSink` is a Flink **Sink V2** implementation that opts into four extension interfaces
(`DynamicIcebergSink.java:66-72`):

```java
public class DynamicIcebergSink
    implements Sink<DynamicRecordInternal>,
        SupportsPreWriteTopology<DynamicRecordInternal>,
        SupportsCommitter<DynamicCommittable>,
        SupportsPreCommitTopology<DynamicWriteResult, DynamicCommittable>,
        SupportsPostCommitTopology<DynamicCommittable> { ... }
```

The full job graph (assembled in `Builder.append()`, `DynamicIcebergSink.java:394-443`, plus the
sink-internal topology hooks):

```
            DataStream<T>  (user input)
                  │
                  ▼
   ┌──────────────────────────────────────────────┐
   │ process: DynamicRecordProcessor<T>            │   "generator"  (uid: -generator)
   │   - runs user DynamicRecordGenerator          │
   │   - resolves table/schema/spec via caches     │
   │   - converts RowData to target schema         │
   │   - computes writerKey                        │
   └──────────────────────────────────────────────┘
        │ main output                    │ side output  "dynamic-table-update-stream"
        │ DynamicRecordInternal          │ (records whose table needs create/evolve;
        │ (already resolved)             │  carries FULL schema+spec JSON)
        │                                ▼
        │                   keyBy(tableName)                     ← serialize updates per table
        │                   ┌──────────────────────────────┐
        │                   │ map: DynamicTableUpdateOperator│   "Updater" (uid: -updater)
        │                   │  - create table / evolve       │
        │                   │    schema+spec in catalog      │
        │                   │  - re-resolve & convert row    │
        │                   └──────────────────────────────┘
        │                                │
        └─────────────── union ──────────┘
                          │
                          ▼  sinkTo(DynamicIcebergSink)   (uid: -sink)
        ┌───────────────────────────────────────────────────────────┐
        │ addPreWriteTopology:  keyBy(DynamicRecordInternal.writerKey)│   (SupportsPreWriteTopology)
        ├───────────────────────────────────────────────────────────┤
        │ DynamicWriter (createWriter)                                │   one TaskWriter per WriteTarget
        │   emits DynamicWriteResult per WriteTarget at checkpoint    │
        ├───────────────────────────────────────────────────────────┤
        │ addPreCommitTopology:                                       │   (SupportsPreCommitTopology)
        │   keyBy(tableName | "__summary")                            │
        │   → DynamicWriteResultAggregator                            │   1 DynamicCommittable / table / ckpt
        ├───────────────────────────────────────────────────────────┤
        │ DynamicCommitter (createCommitter)                          │   (SupportsCommitter)
        │   commits one snapshot per (table, branch, checkpoint)      │
        ├───────────────────────────────────────────────────────────┤
        │ addPostCommitTopology: (no-op)                             │   (SupportsPostCommitTopology)
        └───────────────────────────────────────────────────────────┘
```

Operator-by-operator:

- **`DynamicRecordProcessor`** (`process`) — runs the user generator and resolves each record. On
  the fast path (table+schema+spec already known and compatible) it emits a fully-resolved
  `DynamicRecordInternal` to the main output. On the slow path (table missing, branch missing, spec
  missing, or schema update needed) it either updates inline (`immediateTableUpdate=true`) or
  emits to a **side output** for the updater.
- **`DynamicTableUpdateOperator`** (`map`, keyed by table name) — only in non-immediate mode.
  Keying by table name guarantees **non-concurrent** create/evolve per table. It applies the
  change to the catalog and re-emits the resolved record, which is then `union`-ed back with the
  main stream.
- **`DynamicWriter`** — keyed by `writerKey` (the pre-write topology). Holds many `TaskWriter`s,
  one per `WriteTarget`. Emits a `DynamicWriteResult` per target at checkpoint.
- **`DynamicWriteResultAggregator`** — keyed by table name. Aggregates per-table write results into
  one `DynamicCommittable` per `(table, checkpoint)`, writing the data files into Iceberg manifest
  files (so the committable is compact and checkpointable).
- **`DynamicCommitter`** — commits one Iceberg snapshot per `(table, branch, checkpoint)`.

uid/name conventions: `operatorName(suffix)` and `prefixIfNotNull(uidPrefix, suffix)` prepend the
builder's `uidPrefix` (`DynamicIcebergSink.java:357-359, 451-453`). Note `build()` defaults a null
prefix to `""` (`:367`), so with no prefix the uids become `"-generator"`, `"-updater"`,
`"-sink"`, and the aggregator uses the random `sinkId` (`"<sinkId> Pre Commit"`). The `sinkId` is a
per-sink random UUID (`:98`) that both separates files written by different sinks to the same table
and names the aggregator operator.

---

## 5. Two table-update modes

The slow path (something must be created/evolved) has two shapes, chosen by
`immediateTableUpdate(boolean)` (default `false`):

- **Immediate (`true`)** — `DynamicRecordProcessor` calls `TableUpdater.update(...)` *inline*
  inside the generator operator and emits the resolved record on the main output. Lowest latency,
  but every subtask may hit the catalog, and concurrent updates to the same table are possible
  (handled by optimistic-retry in `TableUpdater`).
- **Deferred / centralized (`false`, default)** — the processor side-outputs the record; it is
  `keyBy(tableName)`-routed to a single `DynamicTableUpdateOperator` instance per table, which
  serializes create/evolve operations (reducing catalog load and avoiding self-conflicts), then
  re-injects the record via `.union(converted)`. The trade-off is potential backpressure on the
  sink (docs "Schema Evolution").

---

## 6. Distribution & routing (`HashKeyGenerator`)

> There is **no** `DynamicKeySelector` / `DynamicPartitionKeyGenerator` class. Routing lives in
> `HashKeyGenerator.java` (386 lines), which reuses the standard
> `org.apache.iceberg.flink.sink.PartitionKeySelector` / `EqualityFieldKeySelector` internally.

`HashKeyGenerator.generateKey(dynamicRecord, tableSchema, tableSpec, overrideRowData)`
(`HashKeyGenerator.java:76-110`) maps a record to an `int writerKey`. It caches one Flink
`KeySelector<RowData,Integer>` per `SelectorKey` (table, branch, schema/spec ids, equality fields).
The **effective** parallelism is `Math.min(dynamicRecord.writeParallelism(), maxWriteParallelism)`
where `maxWriteParallelism` = the operator's `maxNumberOfParallelSubtasks`.

Distribution-mode selection (`getKeySelector`, `HashKeyGenerator.java:112-193`):

| `DistributionMode` | No equality fields | With equality fields |
|---|---|---|
| `NONE` | round-robin (`tableKeySelector`) | `equalityFieldKeySelector` |
| `HASH` (unpartitioned) | warn → round-robin | `equalityFieldKeySelector` |
| `HASH` (partitioned) | `partitionKeySelector` | checks all partition source fields ∈ equality fields, then `partitionKeySelector` |
| `RANGE` | not supported → falls back (round-robin, or equality if identifier fields exist) | — |

`distributionMode == null` is **Forward mode** at the topology level (see below); inside
`HashKeyGenerator` a null mode defaults to `NONE`.

The clever bit is `TargetLimitedKeySelector` (`HashKeyGenerator.java:236-290`): it precomputes
`distinctKeys[]` (salted by `tableName.hashCode()`) such that the inner selector's hash, reduced
`% writeParallelism`, maps through Flink's `KeyGroupRangeAssignment` to a fixed, table-specific
subset of exactly `writeParallelism` writer subtasks. That is how a per-record `writeParallelism`
limit is honored on top of Flink's own keyGroup→subtask assignment from the `keyBy(writerKey)`.

**Forward mode (`distributionMode == null`)**: bypasses the shuffle entirely; the processor sends
records straight to the writer via a forward edge (enables operator chaining) for very high-volume
tables where shuffle cost dominates. In this mode schema updates are always applied immediately,
and the user is responsible for upstream balancing. (Documented under "Distribution Modes →
Forward Mode".)

---

## 7. Implementation deep-dive (class by class)

### 7.1 Builder & topology wiring — `DynamicIcebergSink`

Builder defaults (`DynamicIcebergSink.java:171-185`): `tableCreator = TableCreator.DEFAULT`,
`immediateUpdate = false`, `dropUnusedColumns = false`, `cacheMaximumSize = 100`,
`cacheRefreshMs = 1_000`, `inputSchemasPerTableCacheMaximumSize = 10`, `caseSensitive = true`.

Key Sink V2 methods:

```java
// createWriter — one writer per subtask, with subtask/attempt ids for file naming
public SinkWriter<DynamicRecordInternal> createWriter(InitContext context) {        // :103
  return new DynamicWriter(catalogLoader.loadCatalog(), writeProperties, flinkConfig,
      cacheMaximumSize, new DynamicWriterMetrics(context.metricGroup()),
      context.getTaskInfo().getIndexOfThisSubtask(), context.getTaskInfo().getAttemptNumber());
}

// createCommitter — note overwriteMode/workerPoolSize come from FlinkWriteConf, NOT hardcoded
public Committer<DynamicCommittable> createCommitter(CommitterInitContext context) { // :114
  FlinkWriteConf flinkWriteConf = new FlinkWriteConf(writeProperties, flinkConfig);
  return new DynamicCommitter(catalogLoader.loadCatalog(), snapshotProperties,
      flinkWriteConf.overwriteMode(), flinkWriteConf.workerPoolSize(), sinkId,
      new DynamicCommitterMetrics(context.metricGroup()));
}

// pre-write: hash by writerKey
DataStream<DynamicRecordInternal> distributeDataStream(DataStream<DynamicRecordInternal> input) { // :447
  return input.keyBy(DynamicRecordInternal::writerKey);
}
```

`addPreCommitTopology` (`:142-164`) keys by `"__summary"` (for `CommittableSummary`) or the
committable's `key().tableName()` and runs the `DynamicWriteResultAggregator`.
`addPostCommitTopology` (`:132-134`) is intentionally empty.

> Correction to a common assumption: the committer args are **not** hardcoded to
> `replacePartitions=false / workerPoolSize=10 / prefix="flink-dynamic"`. They come from
> `FlinkWriteConf` (`overwriteMode()`, `workerPoolSize()`), and the thread-pool name is
> `"iceberg-committer-pool-" + sinkId` (`DynamicCommitter.java:102-103`).

### 7.2 Record processing — `DynamicRecordProcessor`

`DynamicRecordProcessor` is both a `ProcessFunction<T, DynamicRecordInternal>` and a
`Collector<DynamicRecord>` (the generator emits into it). `open()` builds a `TableMetadataCache`, a
`HashKeyGenerator` (sized by `maxNumberOfParallelSubtasks`), and — if `immediateUpdate` — a
`TableUpdater`; otherwise it prepares the `OutputTag` `DYNAMIC_TABLE_UPDATE_STREAM`.

`collect(DynamicRecord)` is the core decision:

1. Look up existence, branch, schema (`TableMetadataCache.ResolvedSchemaInfo`), and spec from the
   cache.
2. If table missing **or** branch missing **or** spec missing **or** `compareResult ==
   SCHEMA_UPDATE_NEEDED`:
   - `immediateUpdate`: call `updater.update(...)` then `emit(...)` with the freshly resolved
     schema/converter/spec.
   - else: compute `writerKey` now and **side-output** a `DynamicRecordInternal` carrying the
     record's *original* schema/spec (row not yet converted — the updater will resolve+convert).
3. Otherwise (fast path): `emit(...)` with the cached resolved schema/converter/spec.

`emit(...)` does the public→internal conversion: it converts the row via the `DataConverter`,
computes `writerKey` from the converted row, resolves equality field **names → ids** via
`DynamicSinkUtil.getEqualityFieldIds`, and emits the `DynamicRecordInternal`.
`DynamicSinkUtil.getEqualityFieldIds` falls back to the schema's `identifierFieldIds()` when no
explicit equality fields are given.

### 7.3 Caching — `TableMetadataCache`, `TableSerializerCache`, `LRUCache`

- **`LRUCache<K,V>`** (`LRUCache.java:36-63`) is **not** Caffeine — it is a `LinkedHashMap` in
  access-order with `removeEldestEntry` evicting once `size() > maximumSize`, plus an optional
  eviction callback. (Class doc references Caffeine semantics but the implementation is the
  LinkedHashMap LRU; it is used for the hot path.)
- **`TableMetadataCache`** (`TableMetadataCache.java`) caches, per `TableIdentifier`, the table's
  branches, schema-comparison results, and specs — backed by `LRUCache` (`:90`). Public lookups:
  `exists(id) → Tuple2<Boolean,Exception>` (`:96`), `branch(id, branch)` (`:107`),
  `schema(id, input) → ResolvedSchemaInfo` (`:111`), `spec(id, spec)` (`:115`). It also keeps a
  per-table **input-schema cache** (`inputSchemas`, sized by `inputSchemasPerTableCacheMaximumSize`)
  storing the comparison result for each incoming `Schema` instance — so an unchanged
  `DynamicRecord.schema` reference resolves with zero catalog work. `NOT_FOUND` (`:49`) is the
  sentinel for "no compatible schema". A time-based refresh (`cacheRefreshMs`) bounds staleness for
  missing items. **Reusing the same `Schema` object across records** is the key performance lever.
- **`ResolvedSchemaInfo`** bundles `(resolvedTableSchema, CompareSchemasVisitor.Result,
  DataConverter recordConverter)` — the schema to write as, the comparison verdict, and the
  converter to adapt the row.
- **`TableSerializerCache`** (`TableSerializerCache.java`) caches `RowDataSerializer` / schema / spec
  by `(tableName, schemaId, specId)` for the `DynamicRecordInternalSerializer` so checkpoint
  (de)serialization of internal records does not reload the table.

### 7.4 Schema comparison & evolution — `CompareSchemasVisitor`, `EvolveSchemaVisitor`

`CompareSchemasVisitor.visit(inputSchema, tableSchema, caseSensitive, dropUnusedColumns)` returns a
three-valued `Result` (`CompareSchemasVisitor.java:36-39`):

- **`SAME`** — semantically identical; write directly (`DataConverter.identity()`).
- **`DATA_CONVERSION_NEEDED`** — the table schema can already accept the data after a *row*
  adaptation (e.g. the table has an extra optional column → fill `null`; a wider type → promote).
  No catalog change; a `DataConverter` is built from `FlinkSchemaUtil.convert(input)` →
  `convert(tableSchema)`.
- **`SCHEMA_UPDATE_NEEDED`** — the *table* must evolve to accept the data.

When `SCHEMA_UPDATE_NEEDED`, `EvolveSchemaVisitor.visit(...)` drives Iceberg's `UpdateSchema` API
(`EvolveSchemaVisitor.java`): `addColumn` (`:205-207`), `updateColumn` for type widening /
`updateColumnDoc` (`:210-228`), `makeOptional` semantics, and `deleteColumn` (`:141`, only when
`dropUnusedColumns` is enabled). Matching is **by name** (hence renames are unsupported).

Supported evolutions (docs): add columns; widen types (int→long, float→double); make required
optional; drop columns (off by default). Unsupported: rename columns.

### 7.5 Partition-spec evolution — `PartitionSpecEvolution`

`PartitionSpecEvolution.evolve(currentSpec, targetSpec)` (`PartitionSpecEvolution.java:61`) returns
`PartitionSpecChanges { termsToAdd, termsToRemove, isEmpty() }` (`:89-120`). Fields are compared by
**source column name + transform string** (`specFieldsAreCompatible`, `:128-137`), and changes are
translated into `Term`s (`Expressions.transform(sourceName, transform)`) that `TableUpdater` feeds
to Iceberg's `UpdatePartitionSpec` (`removeField` / `addField`). `checkCompatibility` (`:41`) gives
a boolean pre-check.

### 7.6 Applying changes to the catalog — `TableUpdater`

`TableUpdater.update(...)` (`TableUpdater.java:63-75`) is the single entry that makes the catalog
match a requested `(table, branch, schema, spec)`, returning the resolved
`(ResolvedSchemaInfo, PartitionSpec)`:

```java
findOrCreateTable(tableIdentifier, schema, spec, tableCreator);   // creates namespace+table if missing
findOrCreateBranch(tableIdentifier, branch);                       // creates branch if missing
ResolvedSchemaInfo newSchemaInfo = findOrCreateSchema(id, schema); // SAME/CONVERT/EVOLVE
PartitionSpec newSpec = findOrCreateSpec(id, spec);                // evolve spec if needed
```

Every step is **optimistic-concurrency safe**: creation catches `AlreadyExistsException` and
re-reads (`:95-99`); branch creation catches `CommitFailedException` and checks `refs()`
(`:110-118`); schema/spec commits catch `CommitFailedException`, invalidate the cache, and re-check
whether a *concurrent* update already satisfied the request (`:161-172`, `:207-223`). `findOrCreateTable`
uses the pluggable `TableCreator` (default `TableCreator.DEFAULT`; override via `Builder.tableCreator`
to set per-table properties/location). This is exactly the logic that the
`DynamicTableUpdateOperator` (`map`) runs after the `keyBy(tableName)` so updates per table are
serialized (`DynamicTableUpdateOperator.java:85-103`).

### 7.7 The writer — `DynamicWriter`

`DynamicWriter` (`DynamicWriter.java:55`) implements
`CommittingSinkWriter<DynamicRecordInternal, DynamicWriteResult>`. It keeps:

- `writers: Map<WriteTarget, TaskWriter<RowData>>` — the live writers, one per target.
- `taskWriterFactories: LRUCache<WriteTarget, RowDataTaskWriterFactory>` — **size-bounded** by
  `cacheMaximumSize` (`:82`) so the number of cached factories cannot grow without bound.

`write(...)` (`:88-150`) computes the `WriteTarget` from the record, lazily creates the factory
(loading the table, resolving equality fields, validating upsert constraints — upsert requires
non-empty equality fields and, for partitioned tables, that every partition source field is an
equality field, `:110-124`), builds a `RowDataTaskWriterFactory` with the record's schema/spec/file
format/target size, then writes `element.rowData()`.

`prepareCommit()` (`:173-201`) is called at checkpoint: it `complete()`s every `TaskWriter`,
producing a `WriteResult` (data files + delete files) wrapped in a `DynamicWriteResult` keyed by
`TableKey` + `specId`, updates metrics, and **clears** `writers` (each checkpoint starts fresh).
`flush()` is a no-op (`:152-155`).

### 7.8 Aggregation — `DynamicWriteResultAggregator`

`DynamicWriteResultAggregator` (`DynamicWriteResultAggregator.java:56`) is an
`AbstractStreamOperator` that, keyed by table name, buffers
`resultsByTableKeyAndSpec: Map<TableKey, Map<Integer/*specId*/, Collection<WriteResult>>>`. On
`prepareSnapshotPreBarrier(checkpointId)` (`:96-126`) it, per `TableKey`:

1. writes the buffered data/delete files into **Iceberg manifest files** via
   `FlinkManifestUtil.writeCompletedFiles(...)` (one manifest blob per spec id, `:147-165`), and
2. emits exactly one `DynamicCommittable(tableKey, byte[][] manifests, jobId, operatorUniqueId,
   checkpointId)` wrapped in `CommittableWithLineage`, preceded by a `CommittableSummary`.

Writing files into manifests keeps the checkpointed committable compact (it carries manifest
references, not raw file lists). It caches `ManifestOutputFileFactory` + format version per table in
an `LRUCache` (`:86-87`), appending a random UUID to the file factory to avoid clashes on
cache-eviction re-creation (`:194-196`).

### 7.9 Commit — `DynamicCommitter`

`DynamicCommitter` (`DynamicCommitter.java:76`) implements `Committer<DynamicCommittable>`. Its
contract (Javadoc `:63-73`): one `DynamicCommittable` per (table, branch, checkpoint); no late
checkpoints; no other writer commits to the same branch with the same
jobId-operatorId-checkpointId.

`commit(Collection<CommitRequest<DynamicCommittable>>)` (`:106-161`):

1. Group requests into `Map<TableKey, NavigableMap<checkpointId, List<CommitRequest>>>` (`:122-131`)
   — i.e. **by table/branch, then ordered by checkpoint**. (The inner `List` exists only to
   migrate older Flink state that had multiple requests per checkpoint; 1.12 will drop it.)
2. Per table: load it, read snapshot ancestry, and compute `maxCommittedCheckpointId` from snapshot
   summaries tagged with this job/operator id (`getMaxCommittedCheckpointId`, `:163-182`).
3. Mark already-committed checkpoints as done via `signalAlreadyCommitted()` (`:150-152`) — this is
   the **idempotency / exactly-once** guard on restart.
4. Commit the remaining (uncommitted) checkpoints (`commitPendingRequests`, `:195-240`).

`commitPendingRequests` reads the `DeltaManifests` back into `WriteResult`s ordered by checkpoint,
defends against adding positional deletes to a concurrently-upgraded V3 table (`:219-231`), then
either `replacePartitions` (`overwriteMode`) or `commitDeltaTxn` (the normal `RowDelta` path,
`:272-306`). Each `commitOperation` (`:338-394`) stamps the snapshot summary with
`flink.max-committed-checkpoint-id`, `flink.job-id`, `flink.operator-id`, targets the branch, and
attaches a `MaxCommittedCheckpointIdValidator` (a `SnapshotAncestryValidator`) so a duplicate
retry that finds the checkpoint already committed is **skipped** rather than double-applied
(`:363-380`). User `snapshotProperties` are applied first and overridden by these internal keys.

### 7.10 Serializers

For Flink checkpointing, three `SimpleVersionedSerializer`s exist:

- `DynamicWriteResultSerializer` — write results between writer and aggregator.
- `DynamicCommittableSerializer` — committables (the manifest byte arrays + jobId/operatorId/
  checkpointId + `TableKey`) between aggregator and committer.
- `DynamicRecordInternalSerializer` — the in-flight internal record. It has two wire formats keyed
  by a `writeSchemaAndSpec` flag: the **update side-output** carries full schema+spec JSON, while
  the **main path** carries just `schemaId`/`specId` and reconstructs schema/spec/`RowDataSerializer`
  from the `TableSerializerCache`. This is why `Builder.append()` builds two
  `DynamicRecordInternalType`s — `false` for the main output, `true` for the update side output
  (`DynamicIcebergSink.java:395-396, 418-420`).

### 7.11 Metrics

- `DynamicWriterMetrics` — per-table flush counters/durations and `numRecordsSend`
  (`DynamicWriter.java:149, 180-182`).
- `DynamicCommitterMetrics` — per-table commit duration and commit summary
  (`DynamicCommitter.java:390-393`).

---

## 8. End-to-end workflow of one record

1. A source element `T` enters `DynamicRecordProcessor`; the user `DynamicRecordGenerator.generate`
   emits one or more `DynamicRecord`s into the processor (acting as `Collector`).
2. The processor consults `TableMetadataCache`:
   - **Fast path** (table+branch+spec known, schema `SAME`/`DATA_CONVERSION_NEEDED`): convert the
     row with the cached `DataConverter`, compute `writerKey`, emit `DynamicRecordInternal` on the
     main output.
   - **Slow path** (missing table/branch/spec or `SCHEMA_UPDATE_NEEDED`): either update inline
     (`immediateTableUpdate`) and emit, or side-output to `DynamicTableUpdateOperator` (keyed by
     table) which creates/evolves the table, converts the row, and re-emits; the result is
     `union`-ed back into the main stream.
3. `keyBy(writerKey)` routes the record to a writer subtask (a fixed, table-specific subset of size
   `writeParallelism`).
4. `DynamicWriter` lazily creates/reuses a `TaskWriter` for the record's `WriteTarget` and writes
   the row to a data file (or emits equality/position deletes in upsert mode).
5. **On checkpoint**: `DynamicWriter.prepareCommit()` completes writers → `DynamicWriteResult`s;
   `DynamicWriteResultAggregator` writes those files into manifests and emits one
   `DynamicCommittable` per `(table, checkpoint)`; both are part of Flink's aligned checkpoint and
   are durably stored.
6. **After the checkpoint completes**, `DynamicCommitter.commit(...)` groups committables by
   `(table, branch)`, skips any checkpoint already reflected in the table's snapshot summaries
   (idempotent restart), and commits one Iceberg snapshot per uncommitted `(table, branch,
   checkpoint)` via `RowDelta` (or `ReplacePartitions` in overwrite mode), stamping
   job/operator/checkpoint ids into the snapshot summary.

**Exactly-once across many tables** is preserved because (a) files are only made visible via the
post-checkpoint commit; (b) each commit is idempotent — guarded by `flink.max-committed-checkpoint-id`
in the snapshot summary and the `MaxCommittedCheckpointIdValidator`; and (c) each `(table, branch,
checkpoint)` maps to exactly one snapshot, so a replayed checkpoint after failure is detected and
skipped rather than duplicated.

---

## 9. Schema & partition-spec evolution semantics (summary)

| Change | Behavior |
|---|---|
| Add column | Auto-applied (`addColumn`). |
| Widen type (int→long, float→double, decimal precision) | Auto-applied (`updateColumn`) / handled by `DataConverter`. |
| Make required → optional | Auto-applied. |
| Table has extra optional column not in record | No table change; row gets `null` for it (`DATA_CONVERSION_NEEDED`). |
| Drop column | Disabled by default; opt-in via `dropUnusedColumns(true)`. Re-appearing field becomes a brand-new column. |
| Rename column | **Unsupported** (comparison is name-based). |
| Partition spec add/remove field | Auto-applied via `UpdatePartitionSpec` (compared by source name + transform). |
| Equality fields | Names resolved to ids; fall back to schema `identifierFieldIds()` when unset. Upsert requires non-empty equality fields. |
| Table format upgrade (V2→V3) | Not supported while the job runs; commit guards against adding positional deletes to a concurrently-upgraded table. |

---

## 10. Configuration & tuning

Builder options (`DynamicIcebergSink.Builder`, defaults in §7.1):

| Method | Purpose |
|---|---|
| `forInput(DataStream<T>)` / `generator(DynamicRecordGenerator<T>)` | input + routing function (required). |
| `catalogLoader(CatalogLoader)` | serializable catalog access in tasks (required). |
| `set(k,v)` / `setAll(map)` | any Iceberg write property (e.g. `write.format`, `write.upsert.enabled`, compression). |
| `overwrite(boolean)` | overwrite (ReplacePartitions) mode. |
| `writeParallelism(int)` | writer parallelism (also caps per-record `writeParallelism`). |
| `uidPrefix(String)` | operator uid/name prefix (set it in production). |
| `snapshotProperties(map)` / `setSnapshotProperty(k,v)` | snapshot summary metadata. |
| `toBranch(String)` | default target branch. |
| `immediateTableUpdate(boolean)` | inline vs centralized table updates (default false). |
| `dropUnusedColumns(boolean)` | allow column drops during evolution (default false). |
| `tableCreator(TableCreator)` | custom table creation (properties/location per table). |
| `cacheMaxSize(int)` | size of table-metadata / serializer / factory LRU caches (default 100). |
| `cacheRefreshMs(long)` | staleness bound for missing cache items (default 1000). |
| `inputSchemasPerTableCacheMaxSize(int)` | per-table input-schema comparison cache (default 10). |
| `caseSensitive(boolean)` | field-name match case sensitivity (default true). |

Tuning notes: reuse the same `Schema` instance to maximize input-schema cache hits; prefer the
default (centralized) update mode to bound catalog load unless latency demands `immediateTableUpdate`;
size `cacheMaxSize` to the number of hot `WriteTarget`s a subtask handles (it bounds open
`RowDataTaskWriterFactory`s); sink properties override table properties on conflict (docs "Notes").

---

## 11. Usage example

From `TestDynamicIcebergSink` (`flink/v1.20/.../sink/dynamic/TestDynamicIcebergSink.java:191-211,
1051-1056`), the real `Collector`-based generator + builder:

```java
// 1) A generator that derives table/branch/schema/spec from each input record
private static class Generator implements DynamicRecordGenerator<MyEvent> {
  @Override
  public void generate(MyEvent row, Collector<DynamicRecord> out) {
    TableIdentifier tableIdentifier = TableIdentifier.of(DATABASE, row.tableName);
    Schema schema = row.schema;                 // reuse the same Schema instance for cache hits
    PartitionSpec spec = row.partitionSpec;
    DynamicRecord record =
        new DynamicRecord(
            tableIdentifier,
            row.branch,                          // e.g. SnapshotRef.MAIN_BRANCH
            schema,
            toRowData(schema, row),              // your RowData conversion
            spec,
            spec.isPartitioned() ? DistributionMode.HASH : DistributionMode.NONE,
            10);                                 // writeParallelism cap for this target
    record.setUpsertMode(row.upsert);
    record.setEqualityFields(row.equalityFields); // names; null ⇒ use schema identifier fields
    out.collect(record);
  }
}

// 2) Wire the sink
DynamicIcebergSink.forInput(dataStream)
    .generator(new Generator())
    .catalogLoader(catalogLoader)               // e.g. CatalogLoader.hive(...)
    .writeParallelism(2)
    .immediateTableUpdate(true)                  // or false (default) for centralized updates
    .set("write.format.default", "parquet")
    .append();
```

The official docs' shorthand (lambda generator) form:

```java
DynamicIcebergSink.forInput(dataStream)
    .generator((inputRecord, out) -> out.collect(
        new DynamicRecord(
            TableIdentifier.of("db", "table"), "branch", SCHEMA,
            (RowData) inputRecord, PartitionSpec.unpartitioned(),
            DistributionMode.HASH, 2)))
    .catalogLoader(CatalogLoader.hive("hive", new Configuration(), Map.of()))
    .writeParallelism(10)
    .immediateTableUpdate(true)
    .append();
```

---

## 12. Classic `IcebergSink` vs `DynamicIcebergSink`

| Dimension | `IcebergSink` / `FlinkSink` (static) | `DynamicIcebergSink` |
|---|---|---|
| Tables per sink | One (1:1) | Any number |
| Table/schema/spec known | At job build time | Per record, at runtime |
| Auto-create tables | No | Yes (`TableCreator`) |
| Schema evolution | Manual + redeploy | Automatic (add/widen/optional/drop) |
| Partition-spec evolution | Manual + redeploy | Automatic |
| Routing | N/A | `DynamicRecordGenerator` per record |
| Per-record control (branch, dist mode, parallelism, upsert, equality fields) | No | Yes (via `DynamicRecord`) |
| Sink element type | `RowData`/`Row`/Avro | `DynamicRecordInternal` |
| Exactly-once | Yes | Yes (per table/branch/checkpoint) |
| Stability | Stable | `@Experimental` |
| Per-record overhead | Minimal | Cache lookups (+ catalog on the slow path) |

---

## 13. References

**Source (read for this doc; identical under `flink/v1.20`, `v2.0`, `v2.1`):**
`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/` —
`DynamicIcebergSink.java` (465), `DynamicRecord.java` (144), `DynamicRecordGenerator.java`,
`DynamicRecordInternal.java`, `DynamicRecordInternalSerializer.java`, `DynamicRecordInternalType.java`,
`DynamicRecordProcessor.java` (199), `DynamicTableRecordGenerator.java`, `DataConverter.java` (235),
`HashKeyGenerator.java` (386), `DynamicTableUpdateOperator.java` (104), `TableUpdater.java` (226),
`TableMetadataCache.java` (323), `TableSerializerCache.java` (133), `LRUCache.java` (64),
`CompareSchemasVisitor.java` (286), `EvolveSchemaVisitor.java` (239), `PartitionSpecEvolution.java` (137),
`TableCreator.java`, `TableKey.java`, `WriteTarget.java` (132), `DynamicSinkUtil.java` (65),
`DynamicWriter.java` (223), `DynamicWriterMetrics.java`, `DynamicWriteResult.java`,
`DynamicWriteResultAggregator.java` (222), `DynamicWriteResultSerializer.java`,
`DynamicCommittable.java`, `DynamicCommittableSerializer.java`, `DynamicCommitter.java` (400),
`DynamicCommitterMetrics.java`.
Tests: `.../src/test/java/org/apache/iceberg/flink/sink/dynamic/TestDynamicIcebergSink.java` (usage
example), `TestHashKeyGenerator`, `TestCompareSchemasVisitor`, `TestPartitionSpecEvolution`,
`TestTableMetadataCache`, `TestTableUpdater`, `TestDynamicWriter`, `TestDynamicCommitter`, …

**Docs / web:**
- Iceberg Flink Writes (nightly): <https://iceberg.apache.org/docs/nightly/flink-writes/#flink-dynamic-iceberg-sink> (source: `docs/docs/flink-writes.md`)
- Apache Flink blog — "From Stream to Lakehouse: Kafka Ingestion with the Flink Dynamic Iceberg Sink": <https://flink.apache.org/2025/10/14/from-stream-to-lakehouse-kafka-ingestion-with-the-flink-dynamic-iceberg-sink/>
- Design/proposal issue #11536: <https://github.com/apache/iceberg/issues/11536>

> Accuracy caveat: facts here were taken from the source files and the local `docs/docs/flink-writes.md`.
> Some third-party blog snippets show a `convert(...)`-returning generator; the real interface is the
> `Collector`-based `generate(...)` shown in §3.2/§11. I did not compile the project as part of writing
> this doc.
