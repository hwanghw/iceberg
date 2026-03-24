# Iceberg Changelog View & `.changes` Table: Deep Dive

How Spark exposes row-level changes (INSERT, DELETE, UPDATE) from Iceberg snapshots.

---

## 1. Overview

Iceberg tracks every change as a new snapshot. The changelog features let you see **what changed
between snapshots** — which rows were inserted, deleted, or updated — with CDC-style metadata.

Two ways to access changelogs:

| Feature | `.changes` table suffix | `create_changelog_view` procedure |
|---|---|---|
| Access | `SELECT * FROM db.orders.changes` | `CALL system.create_changelog_view(table => 'db.orders')` |
| Carry-over removal | No (raw changes) | Yes (automatic) |
| Compute updates | No (only INSERT/DELETE) | Yes (`compute_updates => true`) |
| Net changes | No | Yes (`net_changes => true`) |
| Output | Temporary scan result | Named temporary Spark view |
| Use when | Simple append-only CDC | UPDATE detection, COW artifact cleanup |

### Metadata columns added to every changelog row

| Column | Type | Description |
|---|---|---|
| `_change_type` | STRING | `INSERT`, `DELETE`, `UPDATE_BEFORE`, or `UPDATE_AFTER` |
| `_change_ordinal` | INT | Order index across snapshots (0, 1, 2, ...) |
| `_commit_snapshot_id` | LONG | Snapshot ID when this change occurred |

---

## 2. How It Works Internally

### Call chain

```
User: CALL system.create_changelog_view(table => 'db.orders', ...)

→ CreateChangelogViewProcedure.java
  → Loads SparkChangelogTable (table_name.changes)
    → SparkChangelogScanBuilder builds IncrementalChangelogScan
      → BaseIncrementalChangelogScan.java
        → Finds snapshots between start and end (SnapshotUtil.ancestorsBetween)
        → Filters out REPLACE operation snapshots (compaction — not logical changes)
        → For each snapshot, reads data manifests:
          → ManifestEntry.Status.ADDED  → creates AddedRowsScanTask  (_change_type = INSERT)
          → ManifestEntry.Status.DELETED → creates DeletedDataFileScanTask (_change_type = DELETE)
        → Each task carries: changeOrdinal (snapshot position), commitSnapshotId

  → ChangelogRowReader.java
    → For each task: reads data file rows + appends metadata columns
    → Returns: [table_columns..., _change_type, _change_ordinal, _commit_snapshot_id]

  → Post-processing (in CreateChangelogViewProcedure):
    → RemoveCarryoverIterator: removes COW artifacts (DELETE+INSERT of identical rows)
    → ComputeUpdateIterator: converts DELETE/INSERT pairs to UPDATE_BEFORE/UPDATE_AFTER
    → RemoveNetCarryoverIterator: computes net changes across snapshots

  → Creates temporary Spark view with the processed result
```

### Key: How Iceberg distinguishes INSERT vs DELETE

At the file level, Iceberg doesn't track individual rows — it tracks **files**. A manifest entry
has `Status.ADDED` (file was added in this snapshot) or `Status.DELETED` (file was removed).

- **File added** → all rows in that file are `INSERT` changes
- **File deleted** → all rows in that file are `DELETE` changes

For an UPDATE via copy-on-write (INSERT OVERWRITE), Iceberg:
1. Deletes the old data file (containing the original row)
2. Adds a new data file (containing the modified row)

This produces both DELETE and INSERT entries for the same snapshot, which the changelog
post-processing can convert to UPDATE_BEFORE/UPDATE_AFTER.

---

## 3. Examples with Concrete Data

### Setup

```sql
CREATE TABLE db.users (id INT, name STRING) USING iceberg;
```

### Example 1: Simple INSERT

```sql
-- Snapshot 1: Insert initial data
INSERT INTO db.users VALUES (1, 'Alice'), (2, 'Bob');
```

**Changelog result** (`SELECT * FROM db.users.changes ORDER BY _change_ordinal, id`):

```
+----+-------+--------------+-----------------+---------------------+
| id | name  | _change_type | _change_ordinal | _commit_snapshot_id |
+----+-------+--------------+-----------------+---------------------+
|  1 | Alice | INSERT       |               0 |    5889239598709613 |
|  2 | Bob   | INSERT       |               0 |    5889239598709613 |
+----+-------+--------------+-----------------+---------------------+
```

Both rows appear as INSERT with ordinal 0 (first snapshot).

### Example 2: INSERT + then another INSERT

```sql
-- Snapshot 1: (already done above)
-- Snapshot 2: Insert more data
INSERT INTO db.users VALUES (3, 'Charlie');
```

**Changelog result:**

```
+----+---------+--------------+-----------------+---------------------+
| id | name    | _change_type | _change_ordinal | _commit_snapshot_id |
+----+---------+--------------+-----------------+---------------------+
|  1 | Alice   | INSERT       |               0 |    5889239598709613 |
|  2 | Bob     | INSERT       |               0 |    5889239598709613 |
|  3 | Charlie | INSERT       |               1 |    7869769243560997 |
+----+---------+--------------+-----------------+---------------------+
```

`_change_ordinal` increments per snapshot. Snapshot 2 gets ordinal 1.

### Example 3: DELETE

```sql
-- Snapshot 3: Delete a row
DELETE FROM db.users WHERE id = 2;
```

**What happens internally (copy-on-write):**
- The data file containing `(2, 'Bob')` is rewritten WITHOUT that row
- Old file: Status.DELETED → `(1, 'Alice')` DELETE + `(2, 'Bob')` DELETE
- New file: Status.ADDED → `(1, 'Alice')` INSERT

**Raw `.changes` table (before carry-over removal):**

```
+----+-------+--------------+-----------------+
| id | name  | _change_type | _change_ordinal |
+----+-------+--------------+-----------------+
|  1 | Alice | DELETE       |               2 |   ← carry-over (unchanged row)
|  2 | Bob   | DELETE       |               2 |   ← actual delete
|  1 | Alice | INSERT       |               2 |   ← carry-over (unchanged row)
+----+-------+--------------+-----------------+
```

**After `create_changelog_view` (carry-over removal):**

```
+----+------+--------------+-----------------+
| id | name | _change_type | _change_ordinal |
+----+------+--------------+-----------------+
|  2 | Bob  | DELETE       |               2 |
+----+------+--------------+-----------------+
```

The `(1, 'Alice')` DELETE + INSERT pair is recognized as a **carry-over** (same row, same values,
DELETE immediately followed by INSERT) and removed. Only the actual delete of Bob remains.

### Example 4: UPDATE (via INSERT OVERWRITE, Copy-on-Write)

```sql
-- Table has: (1, 'Alice'), (3, 'Charlie')
-- Snapshot 4: Update Alice's name
INSERT OVERWRITE db.users VALUES (1, 'Alicia'), (3, 'Charlie');
-- This overwrites the partition containing both rows
```

**What happens internally:**
- Old file deleted: `(1, 'Alice')` and `(3, 'Charlie')` → both appear as DELETE
- New file added: `(1, 'Alicia')` and `(3, 'Charlie')` → both appear as INSERT

**Raw changes (before processing):**

```
+----+---------+--------------+
| id | name    | _change_type |
+----+---------+--------------+
|  1 | Alice   | DELETE       |   ← actual change
|  3 | Charlie | DELETE       |   ← carry-over
|  1 | Alicia  | INSERT       |   ← actual change
|  3 | Charlie | INSERT       |   ← carry-over
+----+---------+--------------+
```

**After carry-over removal (no compute_updates):**

```
+----+--------+--------------+
| id | name   | _change_type |
+----+--------+--------------+
|  1 | Alice  | DELETE       |
|  1 | Alicia | INSERT       |
+----+--------+--------------+
```

`(3, 'Charlie')` DELETE + INSERT is identical → carry-over removed.
`(1, 'Alice')` DELETE + `(1, 'Alicia')` INSERT differ → kept as actual change.

**After compute_updates (with `identifier_columns => array('id')`):**

```
+----+--------+---------------+
| id | name   | _change_type  |
+----+--------+---------------+
|  1 | Alice  | UPDATE_BEFORE |
|  1 | Alicia | UPDATE_AFTER  |
+----+--------+---------------+
```

The procedure detects that `id=1` has a DELETE followed by INSERT → converts to UPDATE pair.

### Example 5: MERGE INTO

```sql
-- Table has: (1, 'Alicia'), (3, 'Charlie')
-- Snapshot 5: MERGE — update existing, insert new
MERGE INTO db.users t
USING (VALUES (1, 'Alice_V2'), (4, 'Dave')) AS s(id, name)
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.name = s.name
WHEN NOT MATCHED THEN INSERT *;
```

**What happens internally:**
MERGE INTO uses copy-on-write under the hood. The file containing `(1, 'Alicia')` is rewritten:
- Old file deleted: `(1, 'Alicia')`, `(3, 'Charlie')` → DELETE
- New file added: `(1, 'Alice_V2')`, `(3, 'Charlie')` → INSERT
- Another new file: `(4, 'Dave')` → INSERT

**After carry-over removal:**

```
+----+----------+--------------+
| id | name     | _change_type |
+----+----------+--------------+
|  1 | Alicia   | DELETE       |
|  1 | Alice_V2 | INSERT       |
|  4 | Dave     | INSERT       |
+----+----------+--------------+
```

`(3, 'Charlie')` carry-over removed. The MERGE matched row appears as DELETE + INSERT.

**After compute_updates (with `identifier_columns => array('id')`):**

```
+----+----------+---------------+
| id | name     | _change_type  |
+----+----------+---------------+
|  1 | Alicia   | UPDATE_BEFORE |
|  1 | Alice_V2 | UPDATE_AFTER  |
|  4 | Dave     | INSERT        |
+----+----------+---------------+
```

The matched UPDATE is detected as UPDATE_BEFORE/UPDATE_AFTER via `id` matching.
The unmatched INSERT remains as INSERT.

### Example 6: Net Changes Across Multiple Snapshots

When a row is inserted and then deleted across snapshots, `net_changes` cancels them:

```sql
-- Snapshot 1: INSERT (1, 'a'), (2, 'b'), (3, 'c')
-- Snapshot 2: DELETE (2, 'b'), INSERT (4, 'd')
-- Snapshot 3: DELETE (3, 'c'), INSERT (5, 'e')
```

**Without net_changes (full changelog):**

```
+----+------+--------------+-----------------+
| id | name | _change_type | _change_ordinal |
+----+------+--------------+-----------------+
|  1 | a    | INSERT       |               0 |
|  2 | b    | INSERT       |               0 |
|  3 | c    | INSERT       |               0 |
|  2 | b    | DELETE       |               1 |
|  4 | d    | INSERT       |               1 |
|  3 | c    | DELETE       |               2 |
|  5 | e    | INSERT       |               2 |
+----+------+--------------+-----------------+
```

**With `net_changes => true` (final state only):**

```
+----+------+--------------+-----------------+
| id | name | _change_type | _change_ordinal |
+----+------+--------------+-----------------+
|  1 | a    | INSERT       |               0 |
|  4 | d    | INSERT       |               1 |
|  5 | e    | INSERT       |               2 |
+----+------+--------------+-----------------+
```

`(2, 'b')` INSERT + DELETE cancel out. `(3, 'c')` INSERT + DELETE cancel out.
Only the net effect remains: rows 1, 4, 5 were inserted and never deleted.

---

## 4. What Are Carry-Over Rows?

Copy-on-write (COW) operations rewrite entire data files. When you UPDATE or DELETE a single
row, the entire file containing that row is rewritten. Unchanged rows in that file appear as
both DELETE (from old file) and INSERT (in new file) — these are **carry-over rows**.

```
┌────────────────────────────────────────────────────────────────────────┐
│  CARRY-OVER ROW EXAMPLE                                                 │
│                                                                         │
│  Original file (3 rows):              New file after UPDATE row 2:     │
│  ┌─────────────────────┐              ┌─────────────────────┐          │
│  │ (1, 'Alice')        │  ──DELETE──  │ (1, 'Alice')        │ INSERT  │
│  │ (2, 'Bob')          │  ──DELETE──  │ (2, 'Bobby')        │ INSERT  │
│  │ (3, 'Charlie')      │  ──DELETE──  │ (3, 'Charlie')      │ INSERT  │
│  └─────────────────────┘              └─────────────────────┘          │
│                                                                         │
│  Raw changelog: 6 rows (3 DELETEs + 3 INSERTs)                        │
│  Actual change: 1 row (Bob → Bobby)                                   │
│                                                                         │
│  Carry-over rows: (1, 'Alice') and (3, 'Charlie')                     │
│  They appear as DELETE + INSERT with IDENTICAL values.                 │
│  RemoveCarryoverIterator detects and removes them.                     │
│                                                                         │
│  Detection rule: consecutive DELETE + INSERT where ALL non-metadata    │
│  columns are identical → carry-over → remove both rows.               │
└────────────────────────────────────────────────────────────────────────┘
```

**Implementation** (`RemoveCarryoverIterator.java`):
- Requires rows partitioned and sorted by non-metadata columns
- Scans consecutive rows: if a DELETE is followed by an INSERT with identical values → remove both
- Works on any COW operation (INSERT OVERWRITE, MERGE INTO, UPDATE, DELETE)

---

## 5. compute_updates: Converting DELETE+INSERT to UPDATE

Without `compute_updates`, an UPDATE appears as a DELETE of the old row + INSERT of the new row.
With `compute_updates => true`, the procedure detects these pairs using `identifier_columns`
and converts them:

```
┌────────────────────────────────────────────────────────────────────────┐
│  compute_updates TRANSFORMATION                                         │
│                                                                         │
│  identifier_columns = ['id']                                           │
│                                                                         │
│  Input (after carry-over removal):                                     │
│    (1, 'Alice',  DELETE)     ← id=1, old value                        │
│    (1, 'Alicia', INSERT)     ← id=1, new value                        │
│    (4, 'Dave',   INSERT)     ← id=4, new row (no matching DELETE)     │
│                                                                         │
│  Logic: For each identifier key, if there's a DELETE + INSERT pair:    │
│    DELETE → UPDATE_BEFORE                                              │
│    INSERT → UPDATE_AFTER                                               │
│  If only INSERT (no matching DELETE): keep as INSERT                   │
│  If only DELETE (no matching INSERT): keep as DELETE                   │
│                                                                         │
│  Output:                                                               │
│    (1, 'Alice',  UPDATE_BEFORE)                                       │
│    (1, 'Alicia', UPDATE_AFTER)                                        │
│    (4, 'Dave',   INSERT)                                              │
└────────────────────────────────────────────────────────────────────────┘
```

**Implementation** (`ComputeUpdateIterator.java`):
- Rows must be partitioned by `identifier_columns` + `_change_ordinal`
- Sorted by `identifier_columns` + `_change_ordinal` + `_change_type`
- First removes carry-overs, then detects DELETE/INSERT pairs on same identifier → converts

**Limitation:** `compute_updates` and `net_changes` cannot be combined (throws `IllegalArgumentException`).

---

## 6. Limitations

| Limitation | Impact | Workaround |
|---|---|---|
| **MOR delete files not supported** | `UnsupportedOperationException` if any snapshot in range has delete manifests (from MOR operations) | Use COW mode (`write.delete.mode = copy-on-write`) or compact MOR delete files first |
| **REPLACE operations filtered out** | Compaction snapshots (`operation=replace`) don't appear in changelog | By design — compaction doesn't change logical data |
| **Cannot combine net_changes + compute_updates** | Throws `IllegalArgumentException` | Use one or the other |
| **Requires Spark** | `.changes` table and procedure are Spark-only | No Trino/Flink equivalent yet |
| **Identifier columns needed for UPDATE detection** | Without them, UPDATEs appear as separate DELETE + INSERT | Set identifier columns on table or pass via procedure parameter |

---

## 7. The `.changes` Table vs `create_changelog_view`

### `.changes` table (raw, no post-processing)

```sql
-- Returns raw changelog: includes carry-overs, no update detection
SELECT * FROM db.users.changes
OPTIONS ('start-snapshot-id' = '123', 'end-snapshot-id' = '456')
```

Returns every file-level change without carry-over removal. Useful for:
- Append-only tables (no carry-overs to worry about)
- Custom processing where you want raw data

### `create_changelog_view` (processed, recommended)

```sql
CALL system.create_changelog_view(
    table => 'db.users',
    options => map('start-snapshot-id', '123', 'end-snapshot-id', '456'),
    identifier_columns => array('id'),
    compute_updates => true,
    net_changes => false
)
-- Creates temporary view: db.users_changes (default name)
SELECT * FROM db.users_changes;
```

Processing pipeline:
1. Read raw changes from `.changes` table
2. Remove carry-over rows (always)
3. If `compute_updates`: convert DELETE+INSERT pairs → UPDATE_BEFORE/UPDATE_AFTER
4. If `net_changes`: cancel opposite operations across snapshots
5. Create named temporary Spark view

---

## 8. Appendix: Code Examples

### A1. Basic changelog view

```python
# Create changelog view across all snapshots
spark.sql("""
CALL catalog.system.create_changelog_view(
    table => 'db.users'
)
""")

# Query the view
spark.sql("SELECT * FROM db.users_changes ORDER BY _change_ordinal, id").show()
```

### A2. Changelog with snapshot range

```python
# Get snapshot IDs
snapshots = spark.sql("SELECT snapshot_id, committed_at FROM db.users.snapshots ORDER BY committed_at").collect()

# Changelog between two specific snapshots
spark.sql(f"""
CALL catalog.system.create_changelog_view(
    table => 'db.users',
    options => map(
        'start-snapshot-id', '{snapshots[0].snapshot_id}',
        'end-snapshot-id', '{snapshots[-1].snapshot_id}'
    )
)
""")
```

### A3. Changelog with UPDATE detection

```python
# Detect updates using identifier columns
spark.sql("""
CALL catalog.system.create_changelog_view(
    table => 'db.users',
    identifier_columns => array('id'),
    compute_updates => true
)
""")

# Filter for updates only
spark.sql("""
SELECT * FROM db.users_changes
WHERE _change_type IN ('UPDATE_BEFORE', 'UPDATE_AFTER')
ORDER BY _change_ordinal, _change_type
""").show()
```

### A4. Using identifier fields on the table (auto-detected)

```python
# Set identifier fields on the table (used automatically by compute_updates)
spark.sql("ALTER TABLE db.users SET IDENTIFIER FIELDS id")

# Now compute_updates automatically uses 'id' as identifier
spark.sql("""
CALL catalog.system.create_changelog_view(
    table => 'db.users',
    compute_updates => true
)
""")
```

### A5. Net changes (final state only)

```python
# Only see the net effect across all snapshots
spark.sql("""
CALL catalog.system.create_changelog_view(
    table => 'db.users',
    net_changes => true
)
""")

# Result: only rows whose net effect is non-zero
# (insert+delete of same row cancel out)
spark.sql("SELECT * FROM db.users_changes").show()
```

### A6. Custom view name

```python
spark.sql("""
CALL catalog.system.create_changelog_view(
    table => 'db.users',
    changelog_view => 'my_cdc_feed',
    identifier_columns => array('id'),
    compute_updates => true
)
""")

# Use the custom name
spark.sql("SELECT * FROM my_cdc_feed WHERE _change_type = 'INSERT'").show()
```

### A7. Publishing CDC to Kafka

```python
# Read changelog view and publish to Kafka for downstream consumers
cdc_df = spark.sql("""
SELECT id, name, _change_type, _change_ordinal, _commit_snapshot_id
FROM db.users_changes
ORDER BY _change_ordinal
""")

cdc_df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "users_cdc") \
    .save()
```

---

## 9. Key Source Files

| File | Role |
|------|------|
| `spark/.../procedures/CreateChangelogViewProcedure.java` | Procedure entry point — orchestrates carry-over removal, update computation |
| `spark/.../source/SparkChangelogTable.java` | Implements `.changes` table suffix |
| `core/.../BaseIncrementalChangelogScan.java` | Discovers changed files between snapshots (skips REPLACE ops) |
| `api/.../ChangelogScanTask.java` | Interface for changelog scan tasks (AddedRows, DeletedDataFile) |
| `spark/.../source/ChangelogRowReader.java` | Reads data files and appends metadata columns |
| `spark/.../RemoveCarryoverIterator.java` | Detects and removes carry-over rows (identical DELETE+INSERT) |
| `spark/.../ComputeUpdateIterator.java` | Converts DELETE+INSERT pairs to UPDATE_BEFORE/UPDATE_AFTER |
| `spark/.../RemoveNetCarryoverIterator.java` | Computes net changes across multiple snapshots |
| `core/.../MetadataColumns.java` | Defines `_change_type`, `_change_ordinal`, `_commit_snapshot_id` |
| `api/.../ChangelogOperation.java` | Enum: INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER |
| `spark-extensions/.../TestCreateChangelogViewProcedure.java` | Comprehensive test examples |
