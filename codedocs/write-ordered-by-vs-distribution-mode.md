# WRITE ORDERED BY vs write.distribution-mode: Interaction & Precedence

## Overview

When an Iceberg table has a sort order (e.g. from `WRITE ORDERED BY`) and a `write.distribution-mode`
property set to `hash`, there is a conflict: the sort order implies global range distribution, but
the property says hash. **The distribution mode property wins.** The sort order is demoted from a
global guarantee to local ordering within hash partitions.

This document traces through the code to explain exactly how both code paths work and where they diverge.

---

## The Two Scenarios

### Scenario 1: `ALTER TABLE ... WRITE ORDERED BY col` (DDL — no conflict at DDL time)

The `WRITE ORDERED BY` DDL command is **atomic** — it sets **both** the sort order AND overwrites
`write.distribution-mode` to `range` in a single transaction
(`SetWriteDistributionAndOrderingExec.scala:59-64`). So if you had `hash` before, it gets replaced
with `range`. **No conflict exists at DDL time.**

The DDL syntax determines the distribution mode via the parser:

| DDL Syntax | Distribution Mode Set | Sort Order |
|-----------|----------------------|------------|
| `WRITE ORDERED BY col` | `range` | set to `col` |
| `WRITE DISTRIBUTED BY PARTITION` | `hash` | unchanged |
| `WRITE DISTRIBUTED BY PARTITION ORDERED BY col` | `hash` | set to `col` |
| `WRITE LOCALLY ORDERED BY col` | *(not set — unchanged)* | set to `col` |
| `WRITE UNORDERED` | `none` | cleared |

### Scenario 2: Sort order exists + `write.distribution-mode=hash` at write time (real conflict)

This is the real conflict. It arises when:
1. `ALTER TABLE ... WRITE ORDERED BY id` (sets sort order + mode=range)
2. Then `ALTER TABLE SET TBLPROPERTIES ('write.distribution-mode' = 'hash')` (overrides back to hash)

Or equivalently via Java API:
```java
table.replaceSortOrder().asc("id").commit();
table.updateProperties().set(WRITE_DISTRIBUTION_MODE, "hash").commit();
```

At write time, `SparkWriteConf.distributionMode()` reads `write.distribution-mode=hash` from table
properties and returns `HASH`. This feeds into `SparkWriteUtil`:

- **Distribution:** `Distributions.clustered(partitionColumns)` — hash by partition columns only
- **Ordering:** `ordering(table)` — local sort by `[partition_cols, sort_cols]` within each task

The global sort guarantee is **lost**. Data is shuffled by partition hash, then sorted locally.

For **unpartitioned** tables, it's even more extreme: `adjustWriteDistributionMode()`
(`SparkWriteConf.java:308-309`) converts `HASH` to `NONE` since there are no partition columns to
hash by. The sort order still applies locally, but distribution becomes completely unspecified.

---

## Two Code Paths That Establish Sort Order + Distribution

### Path 1: DDL (`ALTER TABLE ... WRITE ORDERED BY`)

```
SQL: ALTER TABLE t WRITE ORDERED BY id

-> Spark SQL parser
  -> IcebergSqlExtensionsAstBuilder.visitSetWriteDistributionAndOrdering()
    -> Determines distributionMode from syntax
    -> Creates SetWriteDistributionAndOrdering logical plan
      -> Executed by SetWriteDistributionAndOrderingExec
        -> Writes BOTH sort order AND write.distribution-mode property in one transaction
```

**Parser decides the distribution mode** based on SQL syntax
(`IcebergSqlExtensionsAstBuilder.scala:234-242`):

```scala
val distributionMode = if (distributionSpec != null) {
  Some(DistributionMode.HASH)           // WRITE DISTRIBUTED BY PARTITION
} else if (orderingSpec.UNORDERED != null) {
  Some(DistributionMode.NONE)           // WRITE UNORDERED
} else if (orderingSpec.LOCALLY() != null) {
  None                                   // WRITE LOCALLY ORDERED BY -> no property change
} else {
  Some(DistributionMode.RANGE)          // WRITE ORDERED BY -> RANGE
}
```

**Executor persists both** in a single transaction
(`SetWriteDistributionAndOrderingExec.scala:45-66`):

```scala
val txn = iceberg.table.newTransaction()

// 1. Replace sort order
val orderBuilder = txn.replaceSortOrder().caseSensitive(SparkUtil.caseSensitive(session))
sortOrder.foreach {
  case (term, SortDirection.ASC, nullOrder) =>
    orderBuilder.asc(term, nullOrder)
  case (term, SortDirection.DESC, nullOrder) =>
    orderBuilder.desc(term, nullOrder)
}
orderBuilder.commit()

// 2. Set distribution mode property
distributionMode.foreach { mode =>
  txn.updateProperties()
    .set(WRITE_DISTRIBUTION_MODE, mode.modeName())   // writes "range" for WRITE ORDERED BY
    .commit()
}

txn.commitTransaction()
```

After this DDL, the table has both a sort order AND `write.distribution-mode=range` in its properties.

### Path 2: Java API (`replaceSortOrder()` only)

```java
table.replaceSortOrder().asc("id").commit();
// No write.distribution-mode property is set
```

This sets the sort order but writes **no** `write.distribution-mode` property. The distribution
mode is determined later at write time by `defaultWriteDistributionMode()`.

---

## Write-Time Resolution: `SparkWriteConf.distributionMode()`

At write time (INSERT, APPEND, etc.), `SparkWriteConf.distributionMode()` determines the actual
distribution (`SparkWriteConf.java:288-323`):

```java
DistributionMode distributionMode() {
    String modeName = confParser.stringConf()
        .option(SparkWriteOptions.DISTRIBUTION_MODE)          // 1. write option (highest priority)
        .sessionConf(SparkSQLProperties.DISTRIBUTION_MODE)    // 2. session conf
        .tableProperty(TableProperties.WRITE_DISTRIBUTION_MODE) // 3. table property
        .parseOptional();

    if (modeName != null) {
        return adjustWriteDistributionMode(DistributionMode.fromName(modeName));
    } else {
        return defaultWriteDistributionMode();  // only if ALL three are null
    }
}
```

### Config precedence chain

1. **Write option** (e.g. `.option("distribution-mode", "hash")` on DataFrameWriter) — highest
2. **Session conf** (`spark.sql.iceberg.distribution-mode`)
3. **Table property** (`write.distribution-mode`)
4. **Default** (`defaultWriteDistributionMode()`) — only if all above are null

### When `defaultWriteDistributionMode()` kicks in

Only when no explicit `write.distribution-mode` is set at any level:

```java
private DistributionMode defaultWriteDistributionMode() {
    if (table.sortOrder().isSorted()) {
        return RANGE;      // auto-detects sort order -> global range sort
    } else if (table.spec().isPartitioned()) {
        return HASH;       // partitioned but unsorted -> hash by partition cols
    } else {
        return NONE;       // unpartitioned and unsorted -> no distribution
    }
}
```

**This is why both paths converge for sorted tables:**
- DDL path: `WRITE ORDERED BY` writes `write.distribution-mode=range` -> property is read back -> RANGE
- Java API path: No property exists -> falls to `defaultWriteDistributionMode()` -> detects sort order -> RANGE

### `adjustWriteDistributionMode()` — safety adjustments

When an explicit mode is found, it may be adjusted (`SparkWriteConf.java:305-313`):

```java
private DistributionMode adjustWriteDistributionMode(DistributionMode mode) {
    if (mode == RANGE && table.spec().isUnpartitioned() && table.sortOrder().isUnsorted()) {
        return NONE;   // RANGE makes no sense without partitioning or sort order
    } else if (mode == HASH && table.spec().isUnpartitioned()) {
        return NONE;   // HASH makes no sense without partition columns
    } else {
        return mode;
    }
}
```

---

## How Distribution Mode Maps to Spark Distribution

`SparkWriteUtil.writeDistribution()` translates the mode into a Spark `Distribution`
(`SparkWriteUtil.java:106-120`):

```java
private static Distribution writeDistribution(Table table, DistributionMode mode) {
    switch (mode) {
        case NONE:  return Distributions.unspecified();           // no shuffle
        case HASH:  return Distributions.clustered(clustering(table));  // hash by partition cols
        case RANGE: return Distributions.ordered(ordering(table));      // global range sort
    }
}
```

`SparkWriteUtil.writeOrdering()` determines local ordering within each task
(`SparkWriteUtil.java:246-252`):

```java
private static SortOrder[] writeOrdering(Table table, boolean fanoutEnabled) {
    if (fanoutEnabled && table.sortOrder().isUnsorted()) {
        return EMPTY_ORDERING;   // fanout writers handle partitioning, no sort needed
    } else {
        return ordering(table);  // local sort by partition cols + sort order cols
    }
}
```

**Key insight:** Distribution and ordering are independent. The distribution controls the shuffle
(how data is partitioned across executors), and the ordering controls local sort within each task.
When mode=HASH, the sort order still applies locally — it's just not globally enforced.

---

## Complete Behavior Matrix

| Sort Order | Distribution Mode | Distribution Result | Ordering Result |
|-----------|------------------|--------------------|--------------------|
| sorted | not set (default) | `ordered(partition+sort)` — global | `[partition, sort]` |
| sorted | range | `ordered(partition+sort)` — global | `[partition, sort]` |
| sorted | hash (partitioned) | `clustered(partition)` — hash only | `[partition, sort]` local |
| sorted | hash (unpartitioned) | `unspecified` (HASH->NONE adjust) | `[sort]` local |
| sorted | none | `unspecified` | `[partition, sort]` local |
| unsorted | not set (partitioned) | `clustered(partition)` — hash | `[partition]` local |
| unsorted | hash (partitioned) | `clustered(partition)` — hash | `[partition]` local |
| unsorted | range (partitioned) | `ordered(partition)` — global | `[partition]` |

---

## Fanout Writers vs Clustered Writers: Who Does the Sorting?

### Two separate layers

The write path has two distinct layers that are often confused:

1. **Spark shuffle/sort layer** — enforced BEFORE data reaches the Iceberg writer. Driven by
   `SparkWriteRequirements` (distribution + ordering) that Spark's V2 write framework physically
   executes via shuffle and sort operators.

2. **Iceberg writer layer** — receives pre-shuffled, pre-sorted rows and writes them to files.
   The writer choice is determined by `fanoutEnabled` (`SparkWrite.java:847-851`):

```java
if (fanoutEnabled) {
    this.delegate = new FanoutDataWriter<>(...);    // keeps multiple files open
} else {
    this.delegate = new ClusteredDataWriter<>(...);  // keeps one file open at a time
}
```

### FanoutDataWriter — a router, not a sorter

`FanoutWriter` (`core/src/main/java/org/apache/iceberg/io/FanoutWriter.java`) maintains a
`Map<StructLike, FileWriter>` — one open file writer per partition. When a row arrives, it looks
up the partition key in the map and writes to the matching file. **It does zero sorting.** It is
purely a router.

```java
// FanoutWriter.java:51-54
public void write(T row, PartitionSpec spec, StructLike partition) {
    FileWriter<T, R> writer = writer(spec, partition);  // lookup from map
    writer.write(row);                                   // route to correct file
}
```

### ClusteredDataWriter — requires pre-sorted input

`ClusteredWriter` (`core/src/main/java/org/apache/iceberg/io/ClusteredWriter.java`) keeps only
**one file writer open at a time**. When a row arrives for a different partition, it closes the
current file and opens a new one. If a row arrives for an **already-closed** partition, it throws:

```
IllegalStateException: Incoming records violate the writer assumption that records are
clustered by spec and by partition within each spec. Either cluster the incoming records
or switch to fanout writers.
```

This is why `ClusteredWriter` requires Spark to sort data by partition columns first — without
that local ordering, records for the same partition could be interleaved and trigger this error.

### Data flow with fanout enabled + sorted table

```
               Spark layer                              Iceberg writer layer
               -----------                              --------------------
Data -> [shuffle by hash/range] -> [local sort by       -> FanoutDataWriter
                                    partition+sort cols]    |
                                                          routes each row to correct
                                                          partition file (files receive
                                                          rows pre-sorted by Spark)
```

Spark does the sorting. The fanout writer just receives pre-sorted rows and routes them.

### Why `writeOrdering()` behaves differently with fanout

The condition `fanoutEnabled && isUnsorted()` means: **only skip ordering when BOTH conditions hold.**

| Fanout | Sort Order | Local Ordering | Why |
|--------|-----------|---------------|-----|
| disabled | unsorted | `[partition_cols]` | ClusteredWriter needs records grouped by partition |
| enabled | unsorted | `EMPTY` | FanoutWriter handles multi-partition routing, no grouping needed |
| disabled | sorted | `[partition_cols, sort_cols]` | ClusteredWriter needs grouping AND user wants sorted data layout |
| enabled | sorted | `[partition_cols, sort_cols]` | FanoutWriter handles routing, but sort order is a user data-layout requirement — only Spark's sort can enforce it |

The fanout writer is purely about **memory vs. ordering tradeoffs** for partition file management.
When `fanoutEnabled=true`, the writer trades higher memory (keeping all partition files open) for
relaxed input ordering requirements. But when the user defines a sort order, that is a semantic
requirement for data layout (e.g., for query performance), not a writer implementation detail —
so Spark must still enforce it regardless of writer type.

---

## Hidden Partitions, PARTITIONED BY, and Default Distribution Mode

### What are hidden partitions?

Iceberg supports **partition transforms** — functions applied to source columns to derive partition
values at write time. The partition column doesn't exist in the schema; it's computed:

```sql
CREATE TABLE events (
    id BIGINT,
    data STRING,
    ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(ts), bucket(16, id))
```

Here `days(ts)` and `bucket(16, id)` are hidden partitions. Users query with
`WHERE ts > '2024-01-01'` — Iceberg automatically applies the `days()` transform for partition
pruning. No need to know the partition column name.

### `PARTITIONED BY` does NOT set `write.distribution-mode`

`CREATE TABLE ... PARTITIONED BY (...)` only creates the partition spec in table metadata. It
writes **no** `write.distribution-mode` property. Only these actions set it:

| Action | Sets `write.distribution-mode`? |
|--------|-------------------------------|
| `CREATE TABLE ... PARTITIONED BY (...)` | No |
| `ALTER TABLE ... WRITE ORDERED BY col` | Yes → `range` |
| `ALTER TABLE ... WRITE DISTRIBUTED BY PARTITION` | Yes → `hash` |
| `ALTER TABLE ... WRITE DISTRIBUTED BY PARTITION ORDERED BY col` | Yes → `hash` |
| `ALTER TABLE ... WRITE LOCALLY ORDERED BY col` | No (unchanged) |
| `ALTER TABLE ... WRITE UNORDERED` | Yes → `none` |
| `ALTER TABLE SET TBLPROPERTIES ('write.distribution-mode' = '...')` | Yes → explicit value |
| Java API: `table.replaceSortOrder().asc("id").commit()` | No |

### `defaultWriteDistributionMode()` — how the default is chosen

When no explicit `write.distribution-mode` exists, `defaultWriteDistributionMode()`
(`SparkWriteConf.java:315-323`) auto-detects from table metadata. **`isSorted()` is checked
before `isPartitioned()`**, so a sort order always wins:

```java
private DistributionMode defaultWriteDistributionMode() {
    if (table.sortOrder().isSorted()) {
        return RANGE;      // sorted wins → global range sort
    } else if (table.spec().isPartitioned()) {
        return HASH;       // partitioned + unsorted → hash by partition cols
    } else {
        return NONE;       // unpartitioned + unsorted → no distribution
    }
}
```

| Table Config | Default Mode | Why |
|---|---|---|
| `PARTITIONED BY (date)` only (unsorted) | **HASH** | `isPartitioned()` → hash by partition cols |
| `PARTITIONED BY (days(ts))` only (unsorted) | **HASH** | same — hidden partition still counts |
| `PARTITIONED BY (date)` + sort order (any source) | **RANGE** | `isSorted()` checked first → RANGE wins |
| `PARTITIONED BY (days(ts))` + sort order | **RANGE** | same — sorted wins over partitioned |
| Unpartitioned + unsorted | **NONE** | nothing to distribute by |
| Unpartitioned + sorted | **RANGE** | `isSorted()` checked first |

### `SortOrderUtil.buildSortOrder()` — partition transforms are prepended to sort order

When the effective sort order is computed, `SortOrderUtil.buildSortOrder()`
(`core/src/main/java/org/apache/iceberg/util/SortOrderUtil.java`) **prepends partition transform
fields** that aren't already in the user's sort order prefix:

```java
// For each partition field not already covered by the sort order prefix:
for (PartitionField field : requiredClusteringFields.values()) {
    String sourceName = schema.findColumnName(field.sourceId());
    builder.asc(Expressions.transform(sourceName, field.transform()));
}
// Then appends the user's sort order fields
```

This means:

| Partition Spec | User Sort Order | Effective Sort Order |
|---|---|---|
| `PARTITIONED BY (days(ts))` | `ORDERED BY id` | `[days(ts) ASC, id ASC]` |
| `PARTITIONED BY (date, bucket(8, data))` | `ORDERED BY id` | `[date ASC, bucket(8, data) ASC, id ASC]` |
| `PARTITIONED BY (date)` | `ORDERED BY id` | `[date ASC, id ASC]` |
| `PARTITIONED BY (date)` | `ORDERED BY date, id` | `[date ASC, id ASC]` (date already in prefix, not duplicated) |

The partition transforms are prepended because data **must** be clustered by partition first (for
correct file layout), then sorted within each partition by the user's sort order. This happens
automatically — the user doesn't need to include partition columns in `WRITE ORDERED BY`.

### How this affects RANGE vs HASH distribution

With **RANGE** distribution (default for sorted tables), `SparkWriteUtil.writeDistribution()` uses
the full effective sort order (partition transforms + user sort order) as the global range
distribution key:

```
PARTITIONED BY (days(ts)), WRITE ORDERED BY id
→ default mode = RANGE
→ distribution = Distributions.ordered([days(ts) ASC, id ASC])   -- global sort
→ ordering     = [days(ts) ASC, id ASC]
```

With **HASH** distribution (default for partitioned unsorted tables), the clustering uses partition
transforms only:

```
PARTITIONED BY (days(ts)), no sort order
→ default mode = HASH
→ distribution = Distributions.clustered([days(ts)])              -- hash by partition
→ ordering     = [days(ts) ASC]                                   -- local (fanout disabled)
→ ordering     = EMPTY                                            -- (fanout enabled)
```

---

## How Compaction Uses Sort Order and Distribution Mode

Compaction (`rewrite_data_files`) has its own relationship with sort order and distribution mode
that differs from normal writes. The key insight: **compaction strategies control their own
shuffle and sort behavior, partly bypassing the table's `write.distribution-mode` setting.**

### Sort strategy: own global RANGE sort, ignores table's distribution mode

`SparkSortFileRewriteRunner` (`spark/.../actions/SparkSortFileRewriteRunner.java`):

```java
// Line 39: reads sort order from table
this.sortOrder = table.sortOrder();

// SparkShufflingFileRewriteRunner line 120: DISABLES table's normal write path
.option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
```

The sort strategy builds its own sort plan with `DistributionAndOrderingUtils.prepareQuery()`,
which uses `Distributions.ordered(ordering)` — always RANGE distribution. It doesn't matter
what `write.distribution-mode` is set to on the table.

### Binpack strategy: no shuffle, but Spark V2 applies local sort

`SparkBinPackFileRewriteRunner` (`spark/.../actions/SparkBinPackFileRewriteRunner.java`):

```java
// Line 57: explicitly sets distribution to NONE (no shuffle)
.option(SparkWriteOptions.DISTRIBUTION_MODE, distributionMode(group).modeName())
// distributionMode() returns NONE unless partition spec changed

// DOES NOT set USE_TABLE_DISTRIBUTION_AND_ORDERING = false
// → defaults to true
```

Because `USE_TABLE_DISTRIBUTION_AND_ORDERING` defaults to `true`, the Spark V2 write path
still calls `SparkWrite.requiredOrdering()` which returns the table's sort order. Spark's
query planner sees `UnspecifiedDistribution` + `requiredOrdering` and adds a
`Sort(global=false)` operator — a **local sort within each task**.

This means binpack output files ARE locally sorted by the table's sort order within each file,
but files from different tasks can have overlapping value ranges (no cross-task coordination).

**This is a subtle but important distinction from "no sorting at all":**

| | Distribution | Sort within file | Cross-file ordering | How |
|---|---|---|---|---|
| **Normal write (RANGE)** | RANGE shuffle | YES | YES — non-overlapping | `SparkWriteConf.writeRequirements()` |
| **Normal write (HASH)** | HASH shuffle | YES (local) | NO — overlapping | `SparkWriteConf.writeRequirements()` |
| **Normal write (NONE)** | No shuffle | YES (local) | NO — overlapping | `SparkWriteConf.writeRequirements()` |
| **Compaction (sort)** | RANGE shuffle | YES | YES — non-overlapping | Own sort plan, `USE_TABLE_DISTRIBUTION_AND_ORDERING=false` |
| **Compaction (binpack)** | No shuffle | YES (local) | NO — overlapping | `DISTRIBUTION_MODE=NONE`, table ordering still applies via V2 |
| **Flink streaming** | No shuffle | NO | NO | Flink doesn't use Spark write path |

### When is `write.distribution-mode` actually used?

| Operation | Uses `write.distribution-mode`? | Why |
|---|---|---|
| Spark batch INSERT/APPEND | **YES** | `SparkWriteConf.distributionMode()` reads the property |
| Spark Structured Streaming | **YES** | Same Spark write path |
| Spark MERGE INTO (COW) | **YES** | Via `copyOnWriteRequirements()` |
| Flink streaming writes | **NO** | Flink has its own write path |
| Compaction (sort strategy) | **NO** | Sets `USE_TABLE_DISTRIBUTION_AND_ORDERING=false`, uses own RANGE sort |
| Compaction (binpack) | **Partially** | Sets `DISTRIBUTION_MODE=NONE` explicitly, but table's `requiredOrdering` still applies as local sort |
| `rewrite_manifests` | **NO** | Metadata-only operation |
| `add_files` | **NO** | Registers existing files, no rewrite |

For **pure Flink streaming tables**, `write.distribution-mode=range` is unused at write time.
It serves as the sort order declaration that compaction reads via `table.sortOrder()`.

---

## Key Source Files

| File | Role |
|------|------|
| `spark/v4.1/spark-extensions/.../IcebergSqlExtensionsAstBuilder.scala` (L222-251) | Parser: determines distributionMode from DDL syntax |
| `spark/v4.1/spark-extensions/.../SetWriteDistributionAndOrderingExec.scala` | Executor: persists sort order + distribution mode property |
| `spark/v4.1/spark/src/.../SparkWriteConf.java` (L288-323) | Write-time: resolves distribution mode from config chain |
| `spark/v4.1/spark/src/.../SparkWriteUtil.java` (L98-260) | Write-time: builds Spark Distribution and SortOrder from mode |
| `spark/v4.1/spark/src/.../source/SparkWrite.java` (L847-851) | Writer selection: FanoutDataWriter vs ClusteredDataWriter |
| `core/src/main/java/org/apache/iceberg/io/FanoutWriter.java` | Fanout writer: routes rows to per-partition files via map lookup |
| `core/src/main/java/org/apache/iceberg/io/ClusteredWriter.java` | Clustered writer: single open file, requires pre-sorted input |
| `core/src/main/java/org/apache/iceberg/util/SortOrderUtil.java` | Prepends partition transforms to user sort order |
| `spark/v4.1/spark/src/.../Spark3Util.java` | Converts Iceberg sort order/partition spec to Spark expressions |
| `spark/v4.1/spark/src/.../SortOrderToSpark.java` | Visitor: converts Iceberg transforms (days, bucket, etc.) to Spark |
| `spark/v4.1/spark/src/test/.../TestSparkDistributionAndOrderingUtil.java` | Tests for all distribution+ordering combinations |
| `spark/v4.1/spark-extensions/src/test/.../TestSetWriteDistributionAndOrdering.java` | Tests for DDL WRITE ORDERED BY behavior |
