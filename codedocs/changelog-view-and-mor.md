# Changelog View in Spark: Architecture & MOR Limitation

## Overview

`CreateChangelogViewProcedure` creates a temporary Spark SQL view that exposes row-level changes
(INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER) between Iceberg snapshots. It does **not** support
Merge-on-Read (MOR) tables — when any snapshot in the requested range contains delete files
(produced by MOR operations), the scan throws:

```
java.lang.UnsupportedOperationException:
    Delete files are currently not supported in changelog scans
```

This document traces through the code to explain how it works and exactly where MOR fails.

---

## End-to-End Call Chain

```
User SQL:
  CALL system.create_changelog_view(table => 'my_table', ...)

→ CreateChangelogViewProcedure.call()                          [spark procedures]
  → BaseProcedure.loadRows(changelogTableIdent, options)       [spark procedures]
    → spark.read().options(options).table("<table>.changes")    [Spark DataSource V2]
      → SparkChangelogTable.newScanBuilder(options)            [spark source]
        → SparkChangelogScanBuilder.build()                    [spark source]
          → table.newIncrementalChangelogScan()                [Iceberg Table API]
            → BaseIncrementalChangelogScan                     [iceberg-core]
              → doPlanFiles()
                → orderedChangelogSnapshots()     ← *** FAILS HERE FOR MOR ***
                → ManifestGroup.plan()
```

---

## Step-by-Step Code Flow

### 1. `CreateChangelogViewProcedure.call()` — Entry Point

**File:** `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/procedures/CreateChangelogViewProcedure.java`

```java
public Iterator<Scan> call(InternalRow args) {
    // ...
    Identifier changelogTableIdent = changelogTableIdent(tableIdent);
    Dataset<Row> df = loadRows(changelogTableIdent, options(input));
    // ... post-processing: remove carry-overs, compute update images, net changes ...
    df.createOrReplaceTempView(viewName);
}
```

The procedure:
1. Resolves the changelog table identifier (e.g., `db.my_table.changes`).
2. Calls `loadRows()` — inherited from `BaseProcedure` — which does
   `spark.read().options(options).table(tableName)`.
3. Post-processes the DataFrame (carry-over removal, update image computation).
4. Registers the result as a temporary view.

The `loadRows()` call triggers Spark's DataSource V2 API, which routes to `SparkChangelogTable`.

### 2. `SparkChangelogTable` → `SparkChangelogScanBuilder` — Building the Scan

**File:** `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkChangelogTable.java`

```java
public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkChangelogScanBuilder(spark(), table, schema, options);
}
```

**File:** `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkChangelogScanBuilder.java`

```java
public Scan build() {
    // ... resolve start/end snapshot IDs from options ...
    IncrementalChangelogScan scan = buildIcebergScan(projection, startSnapshotId, endSnapshotId);
    return new SparkChangelogScan(spark(), table(), scan, readConf(), projection, filters());
}

private IncrementalChangelogScan buildIcebergScan(...) {
    IncrementalChangelogScan scan = table()
        .newIncrementalChangelogScan()
        .caseSensitive(caseSensitive())
        .filter(filter())
        .project(projection);
    if (startSnapshotId != null) scan = scan.fromSnapshotExclusive(startSnapshotId);
    if (endSnapshotId != null) scan = scan.toSnapshot(endSnapshotId);
    return scan;
}
```

This creates a `BaseIncrementalChangelogScan` from the Iceberg core library.

### 3. `SparkChangelogScan.taskGroups()` → `scan.planTasks()` — Executing the Scan

**File:** `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkChangelogScan.java`

```java
private List<ScanTaskGroup<ChangelogScanTask>> taskGroups() {
    if (taskGroups == null) {
        try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> groups = scan.planTasks()) {
            this.taskGroups = Lists.newArrayList(groups);
        }
    }
    return taskGroups;
}
```

When Spark materializes the batch (via `toBatch()` → `taskGroups()`), it calls
`scan.planTasks()` which calls `scan.planFiles()` → `doPlanFiles()`.

### 4. `BaseIncrementalChangelogScan.doPlanFiles()` — The Core Logic

**File:** `core/src/main/java/org/apache/iceberg/BaseIncrementalChangelogScan.java`

```java
protected CloseableIterable<ChangelogScanTask> doPlanFiles(
    Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {

    Deque<Snapshot> changelogSnapshots =
        orderedChangelogSnapshots(fromSnapshotIdExclusive, toSnapshotIdInclusive);
    // ...
    Set<ManifestFile> newDataManifests = /* only data manifests from changelog snapshots */;

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), newDataManifests, ImmutableList.of() /* NO delete manifests */)
            .filterManifestEntries(entry -> changelogSnapshotIds.contains(entry.snapshotId()))
            .ignoreExisting();

    return manifestGroup.plan(new CreateDataFileChangeTasks(changelogSnapshots));
}
```

Key observations:
- It **only** collects **data manifests** (`snapshot.dataManifests()`), never delete manifests.
- The `ManifestGroup` is constructed with `ImmutableList.of()` for delete manifests — meaning
  delete files are explicitly excluded from planning.
- `CreateDataFileChangeTasks` maps manifest entries to:
  - `ADDED` data file entry → `BaseAddedRowsScanTask` (INSERT)
  - `DELETED` data file entry → `BaseDeletedDataFileScanTask` (DELETE)

### 5. `orderedChangelogSnapshots()` — **WHERE MOR FAILS**

**File:** `core/src/main/java/org/apache/iceberg/BaseIncrementalChangelogScan.java`, line ~103–115

```java
private Deque<Snapshot> orderedChangelogSnapshots(Long fromIdExcl, long toIdIncl) {
    Deque<Snapshot> changelogSnapshots = new ArrayDeque<>();

    for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(table(), toIdIncl, fromIdExcl)) {
        if (!snapshot.operation().equals(DataOperations.REPLACE)) {
            if (!snapshot.deleteManifests(table().io()).isEmpty()) {
                throw new UnsupportedOperationException(
                    "Delete files are currently not supported in changelog scans");
            }
            changelogSnapshots.addFirst(snapshot);
        }
    }
    return changelogSnapshots;
}
```

This method iterates through ancestor snapshots in the requested range. For each snapshot:

1. **Skip `REPLACE` operations** — these are file compaction/rewrite operations that don't change
   logical data.
2. **Check for delete manifests** — if the snapshot has **any** delete manifests (meaning it
   contains delete files), it throws `UnsupportedOperationException`.
3. Otherwise, add the snapshot to the changelog.

---

## Why MOR Fails

### Copy-on-Write (COW) — Works ✅

In COW mode, mutations (DELETE, UPDATE, MERGE) are performed by:
1. **Removing** the old data file (manifest entry status = `DELETED`).
2. **Adding** a new data file with the updated content (manifest entry status = `ADDED`).

The snapshot has **no delete manifests** — only data manifests with added/deleted entries.
`orderedChangelogSnapshots()` passes the check, and `doPlanFiles()` correctly generates
`DeletedDataFileScanTask` and `AddedRowsScanTask` from the data manifest entries.

### Merge-on-Read (MOR) — Fails ❌

In MOR mode, mutations are performed by:
1. Writing a **delete file** (position delete or equality delete) that marks rows as deleted.
2. Optionally writing a **new data file** for inserted/updated rows.

The snapshot produced by a MOR operation **has delete manifests** (containing the delete files).
When `orderedChangelogSnapshots()` encounters this snapshot, it hits the explicit guard:

```java
if (!snapshot.deleteManifests(table().io()).isEmpty()) {
    throw new UnsupportedOperationException(
        "Delete files are currently not supported in changelog scans");
}
```

This throws the exact error observed:

```
java.lang.UnsupportedOperationException:
    Delete files are currently not supported in changelog scans
    at o.a.i.BaseIncrementalChangelogScan.orderedChangelogSnapshots(BaseIncrementalChangelogScan.java:109)
    at o.a.i.BaseIncrementalChangelogScan.doPlanFiles(BaseIncrementalChangelogScan.java:60)
    at o.a.i.BaseIncrementalScan.planFiles(BaseIncrementalScan.java:126)
    at o.a.i.BaseIncrementalChangelogScan.planTasks(BaseIncrementalChangelogScan.java:98)
    at o.a.i.spark.source.SparkChangelogScan.taskGroups(SparkChangelogScan.java:117)
    at o.a.i.spark.source.SparkChangelogScan.toBatch(SparkChangelogScan.java:110)
    ...
```

### Why This Limitation Exists

Supporting MOR in changelog scans would require:

1. **Reading delete files** to determine which rows were logically deleted.
2. **Applying delete files against data files** to reconstruct deleted rows (the opposite of what
   a normal read does — instead of filtering them out, you'd need to surface them).
3. **Handling equality deletes** — which don't reference specific rows by position but by column
   values, making it even more complex to determine exactly which rows were affected.

The current `ManifestGroup` + `CreateDataFileChangeTasks` architecture only understands data file
additions and removals. It has no mechanism to read delete file content, join it with data files,
and emit the corresponding DELETE changelog rows.

---

## Component Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                   Spark SQL Layer                                │
│                                                                  │
│  CALL system.create_changelog_view(table => 'T', ...)           │
│                          │                                       │
│                          ▼                                       │
│  ┌──────────────────────────────────────┐                        │
│  │ CreateChangelogViewProcedure.call()  │                        │
│  │  1. loadRows(T.changes, options)     │──── Spark read ────┐   │
│  │  2. remove carry-over rows           │                    │   │
│  │  3. compute update images (optional) │                    │   │
│  │  4. compute net changes (optional)   │                    │   │
│  │  5. createOrReplaceTempView(name)    │                    │   │
│  └──────────────────────────────────────┘                    │   │
│                                                              │   │
│  ┌──────────────────────────────────────┐                    │   │
│  │ SparkChangelogTable                  │◄───────────────────┘   │
│  │  newScanBuilder() →                  │                        │
│  │  SparkChangelogScanBuilder.build()   │                        │
│  │  → SparkChangelogScan                │                        │
│  └──────────────┬───────────────────────┘                        │
│                 │                                                 │
└─────────────────┼────────────────────────────────────────────────┘
                  │
                  │  scan.planTasks()
                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                   Iceberg Core Layer                             │
│                                                                  │
│  ┌──────────────────────────────────────────────┐                │
│  │ BaseIncrementalChangelogScan                  │               │
│  │                                               │               │
│  │  doPlanFiles(fromSnapshot, toSnapshot):       │               │
│  │    1. orderedChangelogSnapshots()             │               │
│  │       - iterate ancestor snapshots            │               │
│  │       - skip REPLACE operations               │               │
│  │       - ❌ THROW if deleteManifests not empty │ ← MOR fails  │
│  │    2. collect newDataManifests                 │               │
│  │    3. ManifestGroup(dataManifests, NO deletes)│               │
│  │    4. plan(CreateDataFileChangeTasks)          │               │
│  │       - ADDED entry  → AddedRowsScanTask      │               │
│  │       - DELETED entry → DeletedDataFileScanTask│              │
│  └──────────────────────────────────────────────┘                │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Summary

| Aspect | Detail |
|--------|--------|
| **Root cause** | `BaseIncrementalChangelogScan.orderedChangelogSnapshots()` explicitly throws `UnsupportedOperationException` when any snapshot in the range contains delete manifests |
| **Error location** | `core/src/main/java/org/apache/iceberg/BaseIncrementalChangelogScan.java:109` |
| **Trigger condition** | Any snapshot produced by a MOR operation (DELETE, UPDATE, MERGE with `write.*.mode=merge-on-read`) will have non-empty delete manifests |
| **COW works because** | COW rewrites entire data files — snapshots only have data manifests with ADDED/DELETED entries |
| **MOR fails because** | MOR writes delete files — snapshots have non-empty delete manifests, hitting the explicit guard |
| **Design gap** | The changelog scan only understands data file additions/removals via `ManifestGroup`; it has no mechanism to read delete file content and reconstruct the affected rows |

