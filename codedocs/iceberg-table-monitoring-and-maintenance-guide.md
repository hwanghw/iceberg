# Iceberg Table Monitoring & Maintenance Decision Guide

Key metrics to monitor for Iceberg table health, the SQL queries to check them,
and the maintenance actions each metric triggers.

---

## 1. Quick Reference: Metric → Threshold → Action

| # | Metric | Healthy | Warning | Critical | Maintenance Action |
|---|--------|---------|---------|----------|-------------------|
| 1 | **Avg data file size** | 128-512 MB | 32-128 MB | < 32 MB | `rewrite_data_files` (binpack or sort) |
| 2 | **Files per partition** | 1-50 | 50-200 | > 200 | `rewrite_data_files` (filter by partition) |
| 3 | **Total data file count** | < 10K | 10K-50K | > 50K | `rewrite_data_files` + review commit frequency |
| 4 | **Delete file count** | 0 | 1-50 | > 50 | `rewrite_data_files` (merges deletes into data files) |
| 5 | **Delete-to-data file ratio** | 0% | 1-10% | > 10% | `rewrite_data_files` with `delete-file-threshold` |
| 6 | **Position delete record count** | 0 | < 1M | > 1M | `rewrite_data_files` (merge position deletes) |
| 7 | **Snapshot count** | < 100 | 100-500 | > 500 | `expire_snapshots` |
| 8 | **Oldest snapshot age** | < 7 days | 7-30 days | > 30 days | `expire_snapshots` |
| 9 | **Manifest count** | < 200 | 200-500 | > 500 | `rewrite_manifests` |
| 10 | **Avg files per manifest** | > 500 | 100-500 | < 100 | `rewrite_manifests` |
| 11 | **Orphan file count** | 0 | any | many | `remove_orphan_files` |
| 12 | **Query planning time** | < 1s | 1-5s | > 5s | All of the above (root-cause analysis) |

---

## 2. Detailed Metrics & Monitoring Queries

### 2.1 Small Files Detection

**Why it matters:** Too many small files increase query planning time (more manifest entries
to parse), reduce scan efficiency (more file open/close overhead), and bloat metadata.

**Root causes:** Streaming ingestion with frequent commits, low-volume partitions, partial
writes from failed jobs.

```sql
-- Overall file size distribution
SELECT
    COUNT(*) AS total_files,
    AVG(file_size_in_bytes) / 1048576 AS avg_file_size_mb,
    MIN(file_size_in_bytes) / 1048576 AS min_file_size_mb,
    MAX(file_size_in_bytes) / 1048576 AS max_file_size_mb,
    PERCENTILE(file_size_in_bytes, 0.5) / 1048576 AS p50_file_size_mb,
    SUM(CASE WHEN file_size_in_bytes < 33554432 THEN 1 ELSE 0 END) AS files_under_32mb,
    SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS files_under_128mb
FROM catalog.db.my_table.files
```

```sql
-- Small files per partition (find worst partitions)
SELECT
    partition,
    COUNT(*) AS file_count,
    AVG(file_size_in_bytes) / 1048576 AS avg_size_mb,
    SUM(file_size_in_bytes) / 1073741824 AS total_size_gb,
    SUM(record_count) AS total_records
FROM catalog.db.my_table.files
GROUP BY partition
HAVING COUNT(*) > 10 AND AVG(file_size_in_bytes) < 134217728  -- >10 files, avg <128 MB
ORDER BY file_count DESC
```

**Thresholds (based on `SizeBasedFileRewritePlanner.java` defaults):**
- Target file size: `write.target-file-size-bytes` = 512 MB (table property default)
- `min-file-size-bytes` for compaction: 75% of target = 384 MB (files below this are rewrite candidates)
- `max-file-size-bytes` for compaction: 180% of target = 921 MB (files above this are split candidates)
- `min-input-files`: 5 (minimum files in a group before compaction triggers)

**Action:**
```sql
-- Compact small files (binpack — no global shuffle, fast)
-- Note: binpack DOES apply local sort within each file (via Spark V2 requiredOrdering),
-- but files from different tasks can have overlapping value ranges.
CALL catalog.system.rewrite_data_files(
    table => 'db.my_table',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '268435456',    -- 256 MB
        'min-file-size-bytes', '67108864',         -- 64 MB (files below this are included)
        'min-input-files', '3'                     -- trigger with just 3 small files
    )
)

-- Compact with sort (global RANGE shuffle — non-overlapping file ranges, best read performance)
-- Sort strategy auto-reads table.sortOrder() — no need to re-specify sort_order unless overriding
CALL catalog.system.rewrite_data_files(
    table => 'db.my_table',
    strategy => 'sort',
    options => map('target-file-size-bytes', '268435456')
)

-- Compaction strategy comparison:
-- | Strategy | Shuffle?   | Sort within file? | Non-overlapping? | Speed   |
-- |----------|-----------|-------------------|------------------|---------|
-- | binpack  | NO        | YES (local sort)  | NO (overlapping) | Fastest |
-- | sort     | YES (RANGE)| YES (global sort) | YES              | Slower  |
-- | zorder   | YES (RANGE)| YES (z-order)     | YES (multi-dim)  | Slowest |
```

---

### 2.2 Delete File Accumulation (MOR Tables)

**Why it matters:** Delete files (position deletes, equality deletes) must be merged at read
time. Each additional delete file adds read overhead. Beyond ~50 delete files per data file,
read performance degrades significantly.

**Root causes:** Merge-on-read UPDATE/DELETE operations, frequent MERGE INTO without compaction.

```sql
-- Delete file overview
SELECT
    COUNT(*) AS total_delete_files,
    SUM(record_count) AS total_delete_records,
    SUM(file_size_in_bytes) / 1048576 AS total_delete_size_mb,
    AVG(record_count) AS avg_records_per_delete_file
FROM catalog.db.my_table.delete_files
```

```sql
-- Delete files per partition
SELECT
    partition,
    COUNT(*) AS delete_file_count,
    SUM(record_count) AS delete_records
FROM catalog.db.my_table.delete_files
GROUP BY partition
ORDER BY delete_file_count DESC
```

```sql
-- Ratio of delete files to data files (overall health indicator)
SELECT
    d.data_file_count,
    del.delete_file_count,
    ROUND(del.delete_file_count * 100.0 / d.data_file_count, 2) AS delete_ratio_pct,
    del.total_delete_records,
    d.total_data_records,
    ROUND(del.total_delete_records * 100.0 / d.total_data_records, 2) AS deleted_records_pct
FROM
    (SELECT COUNT(*) AS data_file_count, SUM(record_count) AS total_data_records
     FROM catalog.db.my_table.files) d,
    (SELECT COUNT(*) AS delete_file_count, SUM(record_count) AS total_delete_records
     FROM catalog.db.my_table.delete_files) del
```

**Thresholds (from `SizeBasedFileRewritePlanner.java`):**
- `delete-file-threshold`: default = MAX_INT (disabled). Recommended: set to 5-10.
- `delete-ratio-threshold`: default = 0.3 (30%). If >30% of a file's rows are deleted, rewrite it.

**Action:**
```sql
-- Compact and merge delete files into data files
CALL catalog.system.rewrite_data_files(
    table => 'db.my_table',
    options => map(
        'delete-file-threshold', '5',       -- rewrite if file has ≥5 associated delete files
        'min-input-files', '1'              -- allow single-file rewrite for delete cleanup
    )
)
```

---

### 2.3 Snapshot Accumulation

**Why it matters:** Each snapshot references a manifest list, which references manifests, which
reference files. Too many snapshots means: larger `metadata.json`, slower table loading, more
files that can't be garbage-collected (every snapshot keeps its files alive).

**Root causes:** High-frequency commits (streaming), no expiration policy, long retention requirements.

```sql
-- Snapshot count and age
SELECT
    COUNT(*) AS snapshot_count,
    MIN(committed_at) AS oldest_snapshot,
    MAX(committed_at) AS newest_snapshot,
    DATEDIFF(MAX(committed_at), MIN(committed_at)) AS span_days
FROM catalog.db.my_table.snapshots
```

```sql
-- Snapshot commit frequency (detect chatty writers)
SELECT
    DATE(committed_at) AS commit_date,
    COUNT(*) AS commits_per_day,
    SUM(CAST(summary['added-data-files'] AS BIGINT)) AS total_added_files,
    SUM(CAST(summary['added-records'] AS BIGINT)) AS total_added_records
FROM catalog.db.my_table.snapshots
GROUP BY DATE(committed_at)
ORDER BY commit_date DESC
LIMIT 14
```

```sql
-- Find snapshots with suspicious patterns (too many small additions)
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['added-data-files'] AS added_files,
    summary['added-records'] AS added_records,
    summary['total-data-files'] AS total_files,
    summary['added-files-size'] AS added_bytes
FROM catalog.db.my_table.snapshots
ORDER BY committed_at DESC
LIMIT 20
```

**Thresholds:**
- `history.expire.max-snapshot-age-ms`: default = 5 days (432,000,000 ms)
- `history.expire.min-snapshots-to-keep`: default = 1
- Recommended: keep 3-7 days for production, 50-200 snapshots min

**Action:**
```sql
-- Expire old snapshots
CALL catalog.system.expire_snapshots(
    table => 'db.my_table',
    older_than => TIMESTAMP '2026-03-16 00:00:00',
    retain_last => 100
)
```

---

### 2.4 Manifest Fragmentation

**Why it matters:** Manifests are the index layer between snapshot metadata and data files. Too
many small manifests increase query planning time (each manifest is a file to read). Too few
large manifests reduce parallelism in planning.

**Root causes:** Many small commits each creating their own manifests, manifest merging disabled.

```sql
-- Manifest overview
SELECT
    COUNT(*) AS manifest_count,
    AVG(added_data_files_count + existing_data_files_count + deleted_data_files_count) AS avg_entries_per_manifest,
    SUM(length) / 1048576 AS total_manifest_size_mb,
    AVG(length) / 1048576 AS avg_manifest_size_mb
FROM catalog.db.my_table.manifests
```

```sql
-- Find small or fragmented manifests
SELECT
    path,
    length / 1024 AS size_kb,
    added_data_files_count,
    existing_data_files_count,
    deleted_data_files_count,
    added_data_files_count + existing_data_files_count + deleted_data_files_count AS total_entries
FROM catalog.db.my_table.manifests
WHERE added_data_files_count + existing_data_files_count + deleted_data_files_count < 100
ORDER BY total_entries ASC
```

**Thresholds (from `TableProperties.java`):**
- `commit.manifest.target-size-bytes`: default = 8 MB
- `commit.manifest.min-count-to-merge`: default = 100 (merge when >100 manifests)
- `commit.manifest-merge.enabled`: default = true

**Action:**
```sql
-- Rewrite manifests for optimal sizing
CALL catalog.system.rewrite_manifests(
    table => 'db.my_table'
)
```

---

### 2.5 Partition Health

**Why it matters:** Over-partitioned tables create too many small partitions with few files each.
Under-partitioned tables have huge partitions that can't be pruned effectively.

```sql
-- Partition-level health summary
SELECT
    partition,
    spec_id,
    file_count,
    record_count,
    total_data_file_size_in_bytes / 1073741824 AS partition_size_gb,
    position_delete_file_count,
    equality_delete_file_count,
    last_updated_at
FROM catalog.db.my_table.partitions
ORDER BY file_count DESC
```

```sql
-- Identify over-partitioned tables (many tiny partitions)
SELECT
    COUNT(*) AS total_partitions,
    AVG(file_count) AS avg_files_per_partition,
    AVG(total_data_file_size_in_bytes) / 1048576 AS avg_partition_size_mb,
    SUM(CASE WHEN total_data_file_size_in_bytes < 104857600 THEN 1 ELSE 0 END) AS partitions_under_100mb,
    SUM(CASE WHEN total_data_file_size_in_bytes > 107374182400 THEN 1 ELSE 0 END) AS partitions_over_100gb
FROM catalog.db.my_table.partitions
```

**Thresholds:**
- Partition size < 100 MB → over-partitioned (consider coarser partition key)
- Partition size 1-10 GB → sweet spot
- Partition size > 100 GB → under-partitioned (consider finer partition key or add partition field)

**Action:** Partition evolution (no maintenance job — schema change):
```sql
-- Add finer partition field
ALTER TABLE db.my_table ADD PARTITION FIELD hours(event_time)

-- Or coarser
ALTER TABLE db.my_table DROP PARTITION FIELD hours(event_time)
ALTER TABLE db.my_table ADD PARTITION FIELD days(event_time)
```

---

### 2.6 Orphan Files

**Why it matters:** Orphan files are data files on storage that are NOT referenced by any
Iceberg snapshot. They accumulate from failed commits, interrupted compaction jobs, or
expired snapshots that haven't been cleaned up. They waste storage.

```sql
-- Estimate orphan files by comparing all_data_files vs actual S3 listing
-- (No direct metadata query — need to compare Iceberg metadata vs object store)
-- Use the procedure directly:

CALL catalog.system.remove_orphan_files(
    table => 'db.my_table',
    older_than => TIMESTAMP '2026-03-16 00:00:00',
    dry_run => true    -- preview only, don't delete
)
-- Returns list of orphan files that WOULD be deleted
```

**Thresholds:** Any orphan files older than the snapshot retention window can be removed.
Use conservative age (7+ days) to avoid deleting files from in-progress commits.

---

### 2.7 Query Planning Time

**Why it matters:** Query planning time is the aggregate symptom of all the above issues.
Slow planning = too many files, manifests, or snapshots to process.

```sql
-- Check Spark metrics after a query (from Spark UI or programmatically)
-- These metrics are from the Iceberg Spark metrics classes:

-- totalPlanningDuration — total time spent in planning (ms)
-- scannedDataManifests — manifests that were actually read
-- skippedDataManifests — manifests pruned by partition stats
-- resultDataFiles — files selected for scan
-- skippedDataFiles — files pruned by column min/max stats
```

**Monitoring approach:** Run a standard benchmark query periodically and track planning duration:

```python
import time

start = time.time()
df = spark.sql("SELECT COUNT(*) FROM catalog.db.my_table WHERE event_time >= current_date() - INTERVAL 1 DAY")
df.collect()
planning_time = time.time() - start

if planning_time > 5.0:
    alert(f"Query planning degraded: {planning_time:.1f}s")
```

**What high planning time indicates:**

| Planning Time | Likely Cause | Fix |
|---|---|---|
| 1-2s | Normal for large tables | No action needed |
| 2-5s | Too many small files or manifests | Compact files + rewrite manifests |
| 5-10s | Excessive snapshots + fragmented manifests | Expire snapshots + rewrite manifests + compact |
| > 10s | Table severely degraded | All maintenance actions + review partition strategy |

---

## 3. Composite Table Health Score

Combine multiple metrics into a single health score for dashboarding and alerting:

```sql
WITH file_stats AS (
    SELECT
        COUNT(*) AS total_files,
        AVG(file_size_in_bytes) AS avg_file_size,
        SUM(CASE WHEN file_size_in_bytes < 33554432 THEN 1 ELSE 0 END) AS small_files
    FROM catalog.db.my_table.files
),
delete_stats AS (
    SELECT
        COUNT(*) AS total_delete_files,
        SUM(record_count) AS total_delete_records
    FROM catalog.db.my_table.delete_files
),
manifest_stats AS (
    SELECT COUNT(*) AS total_manifests
    FROM catalog.db.my_table.manifests
),
snapshot_stats AS (
    SELECT
        COUNT(*) AS total_snapshots,
        MIN(committed_at) AS oldest_snapshot
    FROM catalog.db.my_table.snapshots
)
SELECT
    f.total_files,
    ROUND(f.avg_file_size / 1048576, 1) AS avg_file_size_mb,
    f.small_files,
    d.total_delete_files,
    d.total_delete_records,
    m.total_manifests,
    s.total_snapshots,
    DATEDIFF(current_timestamp(), s.oldest_snapshot) AS oldest_snapshot_days,

    -- Health score (0-100, higher is worse)
    (CASE WHEN f.avg_file_size < 33554432 THEN 30          -- <32 MB avg: critical
          WHEN f.avg_file_size < 134217728 THEN 15         -- <128 MB avg: warning
          ELSE 0 END) +
    (CASE WHEN d.total_delete_files > 50 THEN 25           -- >50 delete files: critical
          WHEN d.total_delete_files > 10 THEN 10           -- >10 delete files: warning
          ELSE 0 END) +
    (CASE WHEN s.total_snapshots > 500 THEN 15             -- >500 snapshots: critical
          WHEN s.total_snapshots > 100 THEN 5              -- >100 snapshots: warning
          ELSE 0 END) +
    (CASE WHEN m.total_manifests > 500 THEN 15             -- >500 manifests: critical
          WHEN m.total_manifests > 200 THEN 5              -- >200 manifests: warning
          ELSE 0 END) +
    (CASE WHEN f.small_files > 1000 THEN 15                -- >1000 small files: critical
          WHEN f.small_files > 100 THEN 5                  -- >100 small files: warning
          ELSE 0 END)
    AS health_score,

    CASE
        WHEN /* health_score */ 0 > 50 THEN 'CRITICAL — immediate maintenance needed'
        WHEN /* health_score */ 0 > 20 THEN 'WARNING — schedule maintenance'
        ELSE 'HEALTHY'
    END AS status

FROM file_stats f, delete_stats d, manifest_stats m, snapshot_stats s
```

**Health score interpretation:**
- **0-20**: Healthy — routine maintenance sufficient
- **20-50**: Warning — schedule maintenance within 24h
- **50+**: Critical — run maintenance immediately

---

## 4. Maintenance Decision Matrix

Given the health metrics, here's which maintenance action to run and in what order:

```
┌────────────────────────────────────────────────────────────────────────┐
│  MAINTENANCE DECISION FLOWCHART                                         │
│                                                                         │
│  Is query planning time > 5s?                                          │
│    YES → Run ALL maintenance actions (full cleanup)                    │
│    NO  → Check individual metrics:                                     │
│                                                                         │
│  1. Delete files > 50 OR delete ratio > 10%?                          │
│     → rewrite_data_files with delete-file-threshold=5                  │
│     → PRIORITY: HIGH (directly impacts read performance)               │
│                                                                         │
│  2. Avg file size < 128 MB OR small files > 100?                      │
│     → rewrite_data_files (binpack for speed, sort for optimization)    │
│     → PRIORITY: HIGH (impacts planning + scan efficiency)              │
│                                                                         │
│  3. Snapshot count > 500 OR oldest > 30 days?                          │
│     → expire_snapshots (keep 7 days / 100 snapshots)                   │
│     → PRIORITY: MEDIUM (metadata bloat, blocks GC)                     │
│                                                                         │
│  4. Manifest count > 500 OR avg entries < 100?                         │
│     → rewrite_manifests                                                │
│     → PRIORITY: MEDIUM (planning time)                                 │
│                                                                         │
│  5. Orphan files exist (older than retention window)?                  │
│     → remove_orphan_files (after expire_snapshots)                     │
│     → PRIORITY: LOW (storage cost only)                                │
│                                                                         │
│  EXECUTION ORDER (when running multiple):                              │
│    1. rewrite_data_files (compaction) — creates new files + snapshots  │
│    2. expire_snapshots — removes old snapshot references               │
│    3. remove_orphan_files — cleans up unreferenced files               │
│    4. rewrite_manifests — optimizes metadata layer                     │
│                                                                         │
│  ⚠ Order matters: expire AFTER compact (so compaction's old files     │
│  can be cleaned up). Orphan removal AFTER expire (so expired files    │
│  become orphans that can be removed).                                  │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Recommended Maintenance Schedule

| Table Type | Compaction | Expire Snapshots | Orphan Cleanup | Rewrite Manifests |
|---|---|---|---|---|
| **Streaming (high ingest)** | Every 1-2 hours | Every 6 hours | Daily | Weekly |
| **Batch (daily ETL)** | After each ETL run | Daily | Weekly | Weekly |
| **MOR (frequent updates)** | Every 4-6 hours | Daily | Weekly | Weekly |
| **Append-only (batch)** | Weekly | Weekly | Monthly | Monthly |
| **Historical (rarely written)** | Monthly | Monthly | Monthly | Quarterly |

---

## 6. Automated Monitoring Script

```python
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()

def check_table_health(table_name):
    """Check all health metrics for an Iceberg table and return recommendations."""

    results = {}

    # 1. File metrics
    file_stats = spark.sql(f"""
        SELECT COUNT(*) AS total_files,
               AVG(file_size_in_bytes) / 1048576 AS avg_mb,
               SUM(CASE WHEN file_size_in_bytes < 33554432 THEN 1 ELSE 0 END) AS small_files
        FROM {table_name}.files
    """).collect()[0]
    results['total_files'] = file_stats.total_files
    results['avg_file_size_mb'] = round(file_stats.avg_mb, 1)
    results['small_files'] = file_stats.small_files

    # 2. Delete file metrics
    delete_stats = spark.sql(f"""
        SELECT COUNT(*) AS count, COALESCE(SUM(record_count), 0) AS records
        FROM {table_name}.delete_files
    """).collect()[0]
    results['delete_files'] = delete_stats.count
    results['delete_records'] = delete_stats.records

    # 3. Snapshot metrics
    snap_stats = spark.sql(f"""
        SELECT COUNT(*) AS count, MIN(committed_at) AS oldest
        FROM {table_name}.snapshots
    """).collect()[0]
    results['snapshots'] = snap_stats.count
    results['oldest_snapshot'] = snap_stats.oldest

    # 4. Manifest metrics
    manifest_stats = spark.sql(f"""
        SELECT COUNT(*) AS count
        FROM {table_name}.manifests
    """).collect()[0]
    results['manifests'] = manifest_stats.count

    # 5. Generate recommendations
    recommendations = []

    if file_stats.avg_mb < 128 or file_stats.small_files > 100:
        recommendations.append(
            f"COMPACT: avg file size {file_stats.avg_mb:.0f} MB, "
            f"{file_stats.small_files} files under 32 MB → run rewrite_data_files")

    if delete_stats.count > 50:
        recommendations.append(
            f"MERGE DELETES: {delete_stats.count} delete files with "
            f"{delete_stats.records} records → run rewrite_data_files with delete-file-threshold=5")

    if snap_stats.count > 500:
        recommendations.append(
            f"EXPIRE SNAPSHOTS: {snap_stats.count} snapshots → run expire_snapshots")

    if manifest_stats.count > 500:
        recommendations.append(
            f"REWRITE MANIFESTS: {manifest_stats.count} manifests → run rewrite_manifests")

    if not recommendations:
        recommendations.append("TABLE HEALTHY — no maintenance needed")

    results['recommendations'] = recommendations
    return results


# Run on all tables
tables = ["catalog.db.orders", "catalog.db.customers", "catalog.db.events"]
for table in tables:
    health = check_table_health(table)
    print(f"\n{'='*60}")
    print(f"TABLE: {table}")
    print(f"  Files: {health['total_files']} (avg {health['avg_file_size_mb']} MB, {health['small_files']} small)")
    print(f"  Delete files: {health['delete_files']} ({health['delete_records']} records)")
    print(f"  Snapshots: {health['snapshots']}")
    print(f"  Manifests: {health['manifests']}")
    for rec in health['recommendations']:
        print(f"  → {rec}")
```

---

## 7. Iceberg Metadata Tables Reference

All metadata tables available via `SELECT * FROM table_name.<metadata_table>`:

| Metadata Table | What It Shows | Key Columns |
|---|---|---|
| `files` | Data files in current snapshot | file_path, file_size_in_bytes, record_count, partition |
| `data_files` | Alias for `files` | Same as `files` |
| `delete_files` | Delete files in current snapshot | file_path, record_count, content (pos/eq) |
| `entries` | Manifest entries (current snapshot) | status (0=EXISTING, 1=ADDED, 2=DELETED), file_path, snapshot_id |
| `all_data_files` | All data files across all snapshots | Same as `files` + snapshot info |
| `all_delete_files` | All delete files across all snapshots | Same as `delete_files` + snapshot info |
| `all_files` | All files (data + delete) | Union of data and delete files |
| `all_entries` | All manifest entries across all snapshots | status, snapshot_id, file_path |
| `manifests` | Manifest files (current snapshot) | path, length, added/existing/deleted file counts |
| `all_manifests` | All manifests across all snapshots | Same + reference_snapshot_id |
| `snapshots` | All snapshots | snapshot_id, committed_at, operation, summary (map) |
| `history` | Snapshot history timeline | made_current_at, snapshot_id, parent_id |
| `refs` | Branches and tags | name, type (BRANCH/TAG), snapshot_id |
| `partitions` | Partition-level statistics | partition, file_count, record_count, total_data_file_size_in_bytes |
| `position_deletes` | Position delete entries | file_path, pos, row |
| `metadata_log_entries` | Metadata file history | timestamp, file (metadata.json location) |

---

## 8. Snapshot Summary Fields Reference

Available in `snapshot.summary` map (access via `summary['field-name']` in SQL):

| Field | Type | Description |
|---|---|---|
| `added-data-files` | count | Files added in this snapshot |
| `deleted-data-files` | count | Files removed in this snapshot |
| `total-data-files` | count | Total files after this snapshot |
| `added-delete-files` | count | Delete files added |
| `removed-delete-files` | count | Delete files removed |
| `total-delete-files` | count | Total delete files |
| `added-records` | count | Records added |
| `deleted-records` | count | Records deleted |
| `total-records` | count | Total records |
| `added-files-size` | bytes | Size of added files |
| `removed-files-size` | bytes | Size of removed files |
| `total-files-size` | bytes | Total size of all files |
| `added-position-deletes` | count | Position delete records added |
| `removed-position-deletes` | count | Position delete records removed |
| `total-position-deletes` | count | Total position delete records |
| `added-equality-deletes` | count | Equality delete records added |
| `removed-equality-deletes` | count | Equality delete records removed |
| `total-equality-deletes` | count | Total equality delete records |
| `changed-partition-count` | count | Partitions affected by this snapshot |
| `manifests-created` | count | New manifests created |
| `manifests-replaced` | count | Manifests replaced (merged) |
| `manifests-kept` | count | Manifests carried forward unchanged |

---

## 9. Avoiding Concurrent Commit Conflicts During Maintenance

The most common operational issue: maintenance jobs fail with `CommitFailedException` because
a concurrent writer committed while the maintenance job was running. Understanding how Iceberg's
optimistic concurrency works is essential for reliable maintenance.

### 9.1 Why Conflicts Happen

Iceberg uses **optimistic concurrency control**. Every commit:
1. Reads the current table metadata (snapshot, manifest list)
2. Plans and executes changes (write new files, create new manifests)
3. Attempts to atomically swap the metadata pointer to the new snapshot
4. If another commit happened between steps 1 and 3 → **conflict detected**

```
┌────────────────────────────────────────────────────────────────────────┐
│  CONFLICT TIMELINE                                                      │
│                                                                         │
│  Time ──────────────────────────────────────────────────▶              │
│                                                                         │
│  Compaction:  [read snap S1] ──── [rewrite files] ──── [commit] ✗     │
│                                                          ↑ CONFLICT   │
│  Writer:            [read snap S1] ── [write] ── [commit snap S2] ✓   │
│                                                                         │
│  Compaction read from S1, but by commit time, S2 exists.              │
│  Iceberg detects that files being replaced may have been modified.    │
│  Throws CommitFailedException → retries from S2.                      │
└────────────────────────────────────────────────────────────────────────┘
```

**What the retry does** (`SnapshotProducer.java:457-548`):
- Does NOT re-read or re-write data files (already written to S3)
- Only retries the **metadata commit** against the new latest snapshot
- Re-validates that the files being replaced still exist and weren't concurrently modified
- If validation passes → commit succeeds on retry
- If validation fails (e.g., files were deleted by another operation) → `ValidationException` → operation aborts

**Specific validations during compaction commit** (`MergingSnapshotProducer.java`):

| Validation | What It Checks | When It Fails |
|---|---|---|
| `validateNoNewDeletesForDataFiles()` | No new delete files were added for files being rewritten | Concurrent MOR DELETE/UPDATE added deletes for same data |
| `validateDeletedDataFiles()` | Files being replaced still exist in current snapshot | Another compaction already replaced the same files |
| `validateAddedDataFiles()` | No new data was added in the same partition filter | Concurrent append to the same partition being compacted |

### 9.2 Retry Configuration

Default retry settings (`TableProperties.java`) and recommended values for high-concurrency tables:

| Property | Default | Recommended (High Concurrency) | Purpose |
|---|---|---|---|
| `commit.retry.num-retries` | 4 | **10-20** | More attempts before giving up |
| `commit.retry.min-wait-ms` | 100 | **200** | Base backoff wait |
| `commit.retry.max-wait-ms` | 60,000 | **120,000** | Max single wait between retries |
| `commit.retry.total-timeout-ms` | 1,800,000 (30 min) | **3,600,000** (1 hour) | Total timeout for all retries |

```sql
-- Increase retry config for a table with frequent concurrent writes
ALTER TABLE db.orders SET TBLPROPERTIES (
    'commit.retry.num-retries' = '20',
    'commit.retry.min-wait-ms' = '200',
    'commit.retry.max-wait-ms' = '120000',
    'commit.retry.total-timeout-ms' = '3600000'
)
```

### 9.3 Best Practices to Minimize Conflicts

#### Practice 1: Compact OLD partitions only (most important)

**The #1 rule:** Never compact partitions that are currently being written to.

If writers append to today's partition, compact yesterday's (or older) partitions:

```sql
-- GOOD: compact yesterday's data (no writers targeting this partition)
CALL catalog.system.rewrite_data_files(
    table => 'db.orders',
    where => 'event_time >= TIMESTAMP ''2026-03-22 00:00:00''
          AND event_time <  TIMESTAMP ''2026-03-23 00:00:00'''
)

-- BAD: compact today's data (writers are actively appending here)
CALL catalog.system.rewrite_data_files(
    table => 'db.orders',
    where => 'event_time >= TIMESTAMP ''2026-03-23 00:00:00'''
)
```

**Why this works:** Writers and compaction operate on **different partitions**. Even though
Iceberg doesn't have partition-level isolation, the validation checks whether files being
replaced were modified — if compaction touches yesterday's files and writers only add to
today's partition, there's no overlap → no conflict.

#### Practice 2: Enable partial progress (smaller commits = smaller conflict window)

Instead of one giant commit at the end, commit each file group as it completes:

```sql
CALL catalog.system.rewrite_data_files(
    table => 'db.orders',
    strategy => 'binpack',
    where => 'event_time < current_date()',
    options => map(
        'partial-progress.enabled', 'true',
        'partial-progress.max-commits', '20',
        'partial-progress.max-failed-commits', '5',   -- tolerate some failures
        'max-file-group-size-bytes', '10737418240'     -- 10 GB per group
    )
)
```

**Why this works:** Each small commit has a shorter validation window. If one group's commit
conflicts, only that group fails — other groups that already committed are preserved. The
operation makes progress even under concurrent writes.

**How it works internally** (`BaseCommitService.java`):
- File rewrite groups are queued as they complete
- Every `rewritesPerCommit` groups, a batch commit is attempted
- If commit fails, the failed group is logged but other groups continue
- `max-failed-commits` controls how many failures are tolerated before aborting

#### Practice 3: Use `binpack` instead of `sort` for streaming tables

`sort` compaction reads ALL files, globally sorts, and writes ALL new files — one huge commit.
`binpack` only groups small files together — many small, independent commits.

```
Sort compaction:   [read ALL files] → [global sort] → [write ALL files] → [ONE big commit]
                   Total time: 30 min → conflict window = 30 min

Binpack compaction: [group 1: 3 files] → [commit 1]
                    [group 2: 5 files] → [commit 2]
                    [group 3: 4 files] → [commit 3]
                    Total time: 30 min → conflict window per commit = 2-3 min
```

**Recommendation:** Use `binpack` for streaming tables with concurrent writes. Reserve `sort`
for batch tables during quiet windows or historical partitions that are never written to.

#### Practice 4: Schedule compaction during low-write windows

If your write pattern has quiet periods (e.g., batch ETL runs hourly, low traffic at 3 AM):

```
Write load:  ████████░░████████░░████████░░████████░░
             ^high    ^low     ^high    ^low
Compact:              ^^^                ^^^
                    (here)             (or here)
```

For streaming tables that write continuously, combine with Practice 1 (compact old partitions).

#### Practice 5: Keep `use-starting-sequence-number` enabled (default)

This is enabled by default (`use-starting-sequence-number = true`). When enabled, compacted
files are written with the **starting snapshot's sequence number**, not the commit snapshot's.

**Why it matters:** If a concurrent MOR operation adds an equality delete at sequence S2,
and compacted files are at sequence S1 (< S2), the delete correctly applies to the compacted
files. Without this, the compacted files would get sequence S2 and the delete would be
invisible to them → **silent data loss**.

```
DO NOT SET THIS TO FALSE unless you understand the implications:
  options => map('use-starting-sequence-number', 'false')  -- DANGEROUS
```

#### Practice 6: Use `where` clause to scope compaction narrowly

The narrower the compaction scope, the less likely it overlaps with concurrent writes:

```sql
-- Narrow scope: compact one specific hour
CALL catalog.system.rewrite_data_files(
    table => 'db.orders',
    where => 'event_time >= ''2026-03-22 14:00:00''
          AND event_time <  ''2026-03-22 15:00:00''
          AND region = ''NA'''
)

-- Broad scope: compact everything (high conflict risk)
CALL catalog.system.rewrite_data_files(table => 'db.orders')
```

#### Practice 7: Rewrite manifests only during quiet periods

`rewrite_manifests` reorganizes the manifest layer. It's a metadata-only operation but
commits a new snapshot. If a writer commits between the manifest read and manifest commit:

- `rewrite_manifests` references manifests that may have been replaced → conflict
- Unlike compaction, there's no `partial-progress` for manifest rewriting
- It's a single all-or-nothing commit

**Recommendation:** Run `rewrite_manifests` during maintenance windows when no writes are active.
If that's impossible, increase retry count and accept occasional failures.

```sql
-- Set high retry count specifically for manifest rewriting sessions
ALTER TABLE db.orders SET TBLPROPERTIES ('commit.retry.num-retries' = '20');

CALL catalog.system.rewrite_manifests(table => 'db.orders');

-- Reset retry count after
ALTER TABLE db.orders SET TBLPROPERTIES ('commit.retry.num-retries' = '4');
```

#### Practice 8: For MOR tables, compact delete files separately

Position delete files accumulate independently from data files. Compacting them has a
smaller blast radius than full data file compaction:

```sql
-- Rewrite only files with many associated delete files
CALL catalog.system.rewrite_data_files(
    table => 'db.orders',
    options => map(
        'delete-file-threshold', '5',    -- only rewrite files with ≥5 delete files
        'min-input-files', '1',          -- allow single-file rewrite
        'partial-progress.enabled', 'true'
    )
)
```

This targets only the most degraded files (those with many deletes) rather than rewriting
everything, reducing the commit surface area.

### 9.4 Conflict Summary by Maintenance Operation

| Operation | Conflict Risk | Can Retry? | Partial Progress? | Best Practice |
|---|---|---|---|---|
| `rewrite_data_files` (binpack) | Medium | Yes (auto) | Yes | Compact old partitions + partial progress |
| `rewrite_data_files` (sort) | High (long-running) | Yes (auto) | Yes | Run on historical partitions or quiet windows |
| `rewrite_manifests` | Medium | Yes (auto) | **No** | Run during quiet windows only |
| `expire_snapshots` | Low | Yes (auto) | N/A | Safe to run anytime (doesn't modify data) |
| `remove_orphan_files` | **None** | N/A | N/A | No Iceberg commit involved — pure S3 deletes |

### 9.5 Error Handling in Orchestration (Airflow Example)

```python
from airflow.decorators import task
from airflow.exceptions import AirflowException

@task(retries=3, retry_delay=timedelta(minutes=5))
def compact_partition(table, partition_filter):
    """Compact a specific partition with conflict-safe settings."""
    try:
        spark.sql(f"""
            CALL catalog.system.rewrite_data_files(
                table => '{table}',
                strategy => 'binpack',
                where => "{partition_filter}",
                options => map(
                    'partial-progress.enabled', 'true',
                    'partial-progress.max-commits', '10',
                    'partial-progress.max-failed-commits', '3',
                    'target-file-size-bytes', '268435456'
                )
            )
        """)
    except Exception as e:
        if "CommitFailedException" in str(e):
            # Expected under high concurrency — Airflow will retry
            raise AirflowException(f"Compaction conflict, will retry: {e}")
        raise  # Unexpected error — fail immediately

# Compact yesterday's partitions (safe — no concurrent writes)
compact_partition(
    "db.orders",
    "event_time >= '2026-03-22' AND event_time < '2026-03-23'"
)
```

---

## 10. Key Source Files

| File | What It Contains |
|---|---|
| `core/.../MetadataTableType.java` | All 16 metadata table types |
| `core/.../SnapshotSummary.java` | All snapshot summary field constants |
| `core/.../TableProperties.java` | All table properties including maintenance/retry thresholds |
| `core/.../PartitionsTable.java` | Partition-level statistics schema |
| `core/.../actions/SizeBasedFileRewritePlanner.java` | Compaction threshold defaults |
| `core/.../SnapshotProducer.java` (L457-548) | Commit retry loop with exponential backoff |
| `core/.../MergingSnapshotProducer.java` (L357-910) | All conflict validation methods |
| `core/.../actions/BaseCommitService.java` (L212-237) | Partial progress commit logic |
| `core/.../actions/RewriteDataFilesCommitManager.java` | Compaction commit with `validateFromSnapshot` |
| `api/.../exceptions/CommitFailedException.java` | Retryable commit conflict exception |
| `spark/.../source/metrics/` | All Spark custom metrics (57+ metrics) |
| `api/.../actions/ExpireSnapshots.java` | Snapshot expiration API |
| `api/.../actions/RewriteDataFiles.java` | Compaction API with partial progress options |
