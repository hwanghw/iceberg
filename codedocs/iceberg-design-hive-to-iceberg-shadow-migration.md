# Design Problem: Shadow Migration from Hive/Parquet to Iceberg

Migrate a production data lakehouse from Hive-style Parquet tables to Apache Iceberg
without downtime, data loss, or disruption to downstream consumers. Uses a 4-phase
shadow migration approach with progressive cutover.

---

## 1. Requirements Gathering (Key Questions)

| # | Question | Expected Answer | Design Impact |
|---|----------|----------------|---------------|
| 1 | How many **tables** and what's the **total data volume**? | 500 tables, 200 TB total, largest table 30 TB | Prioritize: migrate critical tables first; small tables can use `migrate` in-place |
| 2 | What's the **write pattern**? (batch ETL cadence, streaming?) | Daily/hourly Spark batch ETL; 50 tables have streaming Flink writers | Dual-write strategy differs: batch can replay; streaming needs Kafka-based fanout |
| 3 | How many **downstream consumers** read these tables? | ~200 Spark jobs, 50 Trino dashboards, 20 ML pipelines | Must not break existing queries; need read-path compatibility during transition |
| 4 | What's the **acceptable migration window** per table? | Zero downtime for Tier-1 tables; 1-hour window for Tier-3 | Shadow migration for Tier-1; in-place `migrate` for Tier-3 |
| 5 | What **catalog** is used today and what's the target? | Today: Hive Metastore. Target: REST Catalog (or Glue) | Must support both catalogs during transition period |
| 6 | Are there **schema or partitioning changes** planned alongside migration? | Yes — want to adopt hidden partitions and sort orders | Phase 2 is the opportunity to redesign partitioning |
| 7 | What **validation** is needed to confirm migration success? | Row counts match, checksums match, query results identical | Need automated validation framework comparing old vs new |

---

## 2. Capacity Estimation

```
Table inventory:
  total_tables         = 500
  total_data           = 200 TB
  tier_1 (critical)    = 50 tables, 150 TB  → shadow migration (4-phase)
  tier_2 (important)   = 150 tables, 40 TB  → shadow migration (simplified)
  tier_3 (low-risk)    = 300 tables, 10 TB  → in-place migrate

Migration metadata overhead:
  snapshot_table creates: manifests + manifest lists + metadata.json
  Per table: ~1-5 MB metadata for every 1 TB of data
  Total metadata for 200 TB: ~200-1000 MB (negligible)

Dual-write overhead (Phase 2-3):
  Storage: near-zero (Iceberg references same Parquet files via snapshot_table)
  Compute: +20-30% for dual-write Spark jobs (write to both Hive and Iceberg)
  Duration: 2-4 weeks per tier

Timeline:
  Phase 1 (preparation):    2 weeks
  Phase 2 (shadow writes):  4 weeks (Tier-1 first, then Tier-2)
  Phase 3 (shadow reads):   2 weeks
  Phase 4 (decommission):   2 weeks
  Total:                    ~10 weeks for full migration
```

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│              4-PHASE SHADOW MIGRATION: HIVE/PARQUET → ICEBERG            │
│                                                                          │
│  PHASE 1: WRITE OLD, READ OLD (baseline)                                │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │ ETL Jobs  │───▶│ Hive/Parquet │───▶│  Downstream  │                   │
│  │ (Spark)   │    │   Tables     │    │  Consumers   │                   │
│  └──────────┘    └──────────────┘    └──────────────┘                   │
│                                                                          │
│  PHASE 2: WRITE OLD+NEW, READ OLD (shadow writes)                       │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │ ETL Jobs  │─┬─▶│ Hive/Parquet │───▶│  Downstream  │                   │
│  │ (Spark)   │ │  │   Tables     │    │  Consumers   │                   │
│  └──────────┘ │  └──────────────┘    └──────────────┘                   │
│               │  ┌──────────────┐    ┌──────────────┐                   │
│               └─▶│   Iceberg    │───▶│  Validation  │                   │
│                  │   Tables     │    │  Framework   │                   │
│                  └──────────────┘    └──────────────┘                   │
│                                                                          │
│  PHASE 3: WRITE OLD+NEW, READ NEW (cutover reads)                       │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │ ETL Jobs  │─┬─▶│ Hive/Parquet │───▶│  (fallback)  │                   │
│  │ (Spark)   │ │  │   Tables     │    └──────────────┘                   │
│  └──────────┘ │  └──────────────┘                                       │
│               │  ┌──────────────┐    ┌──────────────┐                   │
│               └─▶│   Iceberg    │───▶│  Downstream  │                   │
│                  │   Tables     │    │  Consumers   │                   │
│                  └──────────────┘    └──────────────┘                   │
│                                                                          │
│  PHASE 4: WRITE NEW, READ NEW (decommission old)                        │
│                  ┌──────────────┐    ┌──────────────┐                   │
│  ┌──────────┐   │   Iceberg    │───▶│  Downstream  │                   │
│  │ ETL Jobs  │──▶│   Tables     │    │  Consumers   │                   │
│  │ (Spark)   │   │              │    │              │                   │
│  └──────────┘   └──────────────┘    └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Phase Details

### Phase 1: Write Old, Read Old (Baseline & Preparation)

**Goal:** Establish baseline metrics, inventory all tables, build validation framework. No
changes to production systems.

**Duration:** ~2 weeks

**Actions:**

```
┌────────────────────────────────────────────────────────────────────────┐
│  PHASE 1 CHECKLIST                                                      │
│                                                                         │
│  1. INVENTORY all Hive/Parquet tables                                  │
│     - Table name, location, size, partition scheme, row count          │
│     - Read/write frequency, downstream consumers                       │
│     - Tier classification (1/2/3)                                      │
│                                                                         │
│  2. BASELINE METRICS for each table                                    │
│     - Row counts per partition                                         │
│     - Column checksums (SUM, COUNT DISTINCT) on key columns            │
│     - Query performance benchmarks (P50, P95, P99 latency)            │
│     - File count, average file size, partition count                   │
│                                                                         │
│  3. DESIGN target Iceberg schema                                       │
│     - Hidden partitions (e.g., days(ts) instead of dt=YYYY-MM-DD)     │
│     - Sort order (WRITE ORDERED BY key columns)                        │
│     - Write distribution mode (RANGE vs HASH)                          │
│     - Table properties (format-version=2, compression=zstd)           │
│                                                                         │
│  4. BUILD validation framework                                         │
│     - Automated row count comparison (old vs new)                     │
│     - Column-level checksum comparison                                │
│     - Query result comparison (run same query on both, diff results)  │
│     - Latency comparison dashboard                                     │
│                                                                         │
│  5. SET UP target catalog                                              │
│     - REST Catalog or Glue Catalog for Iceberg tables                 │
│     - Ensure all engines (Spark, Trino, Flink) can access both        │
│       Hive Metastore (old) and Iceberg catalog (new)                  │
│                                                                         │
│  6. TEST migration on a non-critical Tier-3 table (dry run)           │
│     - Run snapshot_table and migrate on a test table                  │
│     - Validate results                                                │
│     - Document lessons learned                                         │
└────────────────────────────────────────────────────────────────────────┘
```

**No Iceberg APIs used yet** — this phase is pure planning and instrumentation.

---

### Phase 2: Write Old + New, Read Old (Shadow Writes)

**Goal:** Start writing to Iceberg tables in parallel with Hive tables. Downstream consumers
still read from Hive (zero risk). Validate that Iceberg tables are consistent with Hive.

**Duration:** ~4 weeks

**Two sub-steps:**

#### Step 2A: Bootstrap — Import Historical Data into Iceberg

Use **`snapshot_table`** to create Iceberg tables that reference existing Hive Parquet files.
This is the key API for shadow migration because:
- Source Hive table is **NOT modified** (production continues uninterrupted)
- **No data is copied** — Iceberg manifests reference the original Parquet files in-place
- Sets `gc.enabled=false` on the Iceberg table (prevents accidental cleanup of shared files)

```
┌────────────────────────────────────────────────────────────────────────┐
│  STEP 2A: BOOTSTRAP WITH snapshot_table                                │
│                                                                         │
│  For each Tier-1 table:                                                │
│                                                                         │
│  CALL catalog.system.snapshot(                                         │
│    source_table => 'spark_catalog.db.orders',    -- Hive source       │
│    table => 'iceberg_catalog.db.orders',          -- Iceberg target    │
│    properties => map(                                                  │
│      'format-version', '2',                                            │
│      'write.parquet.compression-codec', 'zstd'                        │
│    )                                                                   │
│  )                                                                     │
│                                                                         │
│  What happens under the hood:                                          │
│  1. Reads file listing from Hive Metastore (partitions + files)       │
│  2. Reads Parquet footers to extract column statistics                 │
│  3. Creates Iceberg manifests referencing original Parquet files       │
│  4. Creates metadata.json in new Iceberg table location               │
│  5. Registers table in Iceberg catalog                                │
│                                                                         │
│  Result: Iceberg table exists, queryable, pointing to same files      │
│  as Hive table. Both Hive and Iceberg reads return identical data.    │
│                                                                         │
│  ⚠ Source must be in spark_catalog (Hive Metastore)                   │
│  ⚠ GC is disabled on snapshot table (gc.enabled=false)                │
│  ⚠ No data is copied — just metadata references                      │
└────────────────────────────────────────────────────────────────────────┘
```

**Why `snapshot_table` and NOT `migrate`?**

| | `snapshot_table` | `migrate` |
|---|---|---|
| Source table preserved? | **YES** — unchanged | NO — renamed to backup |
| Production impact? | **Zero** | Table temporarily unavailable during rename |
| Rollback? | Drop Iceberg table | Rename backup back |
| Suitable for Phase 2? | **YES** — old system continues | NO — breaks old system |

`migrate` is suitable for Phase 4 (decommission) when you're ready to fully cut over
and don't need the old table anymore.

#### Step 2A-alt: For tables where `snapshot_table` doesn't work

If the source is not in `spark_catalog` (e.g., external Parquet files), use **`add_files`**:

```sql
-- First create the Iceberg table with target schema
CREATE TABLE iceberg_catalog.db.orders (
    order_id BIGINT, customer_id BIGINT, ...
) USING iceberg
PARTITIONED BY (days(order_time))

-- Then import existing Parquet files
CALL iceberg_catalog.system.add_files(
    table => 'db.orders',
    source_table => '`parquet`.`s3://warehouse/db/orders`',
    parallelism => 8
)
```

#### Step 2B: Enable Dual-Write Pipeline

After bootstrapping, modify ETL jobs to write to **both** Hive and Iceberg:

```
┌────────────────────────────────────────────────────────────────────────┐
│  STEP 2B: DUAL-WRITE PIPELINE                                          │
│                                                                         │
│  Option A: Application-level dual write (recommended for batch ETL)    │
│  ┌──────────────────────────────────────────────────────┐              │
│  │  // Existing ETL job modified:                        │              │
│  │  val df = spark.read...transform...                   │              │
│  │                                                       │              │
│  │  // Write 1: Original Hive table (production)         │              │
│  │  df.write.mode("append")                              │              │
│  │    .insertInto("spark_catalog.db.orders")             │              │
│  │                                                       │              │
│  │  // Write 2: New Iceberg table (shadow)               │              │
│  │  df.writeTo("iceberg_catalog.db.orders").append()     │              │
│  │                                                       │              │
│  │  // Validate: compare counts                          │              │
│  │  val hiveCount = spark.table("spark_catalog.db.orders")│             │
│  │    .filter("dt = '2026-03-23'").count()               │              │
│  │  val iceCount = spark.table("iceberg_catalog.db.orders")│            │
│  │    .filter("order_time >= '2026-03-23'").count()       │             │
│  │  assert(hiveCount == iceCount)                         │              │
│  └──────────────────────────────────────────────────────┘              │
│                                                                         │
│  Option B: Kafka-based fanout (for streaming tables)                   │
│  ┌──────────────────────────────────────────────────────┐              │
│  │  Kafka topic "orders"                                 │              │
│  │    ├── Consumer Group 1: Flink → Hive (existing)      │              │
│  │    └── Consumer Group 2: Flink → Iceberg (shadow)     │              │
│  │                                                       │              │
│  │  Both consume same events; write to different sinks.  │              │
│  │  Iceberg sink uses Flink Iceberg connector.           │              │
│  └──────────────────────────────────────────────────────┘              │
│                                                                         │
│  Option C: add_files after each Hive write (low-effort alternative)    │
│  ┌──────────────────────────────────────────────────────┐              │
│  │  // After Hive ETL job completes:                     │              │
│  │  CALL iceberg_catalog.system.add_files(               │              │
│  │    table => 'db.orders',                              │              │
│  │    source_table => 'spark_catalog.db.orders',         │              │
│  │    partition_filter => map('dt', '2026-03-23')        │              │
│  │  )                                                    │              │
│  │                                                       │              │
│  │  ⚠ Adds references to same files — no data copy     │              │
│  │  ⚠ Simpler than dual-write but tightly coupled       │              │
│  │  ⚠ Cannot redesign partition layout (same files)     │              │
│  └──────────────────────────────────────────────────────┘              │
└────────────────────────────────────────────────────────────────────────┘
```

**Choosing between Options A, B, C:**

| Approach | Best for | Partition redesign? | Compute overhead | Coupling |
|---|---|---|---|---|
| **A: App dual-write** | Batch ETL (most common) | YES — write to new partition layout | +20-30% | Low (independent writes) |
| **B: Kafka fanout** | Streaming tables | YES — Iceberg sink has own config | +1 consumer group | Low |
| **C: add_files** | Quick win, no redesign needed | NO — references same files | Minimal | High (depends on Hive write completing) |

**Validation during Phase 2:**

Run after every ETL cycle:
```sql
-- Row count comparison
SELECT 'hive' as source, COUNT(*) as cnt FROM spark_catalog.db.orders WHERE dt = '2026-03-23'
UNION ALL
SELECT 'iceberg' as source, COUNT(*) as cnt FROM iceberg_catalog.db.orders
  WHERE order_time >= '2026-03-23' AND order_time < '2026-03-24'

-- Checksum comparison
SELECT 'hive' as source, SUM(order_total) as total, COUNT(DISTINCT customer_id) as customers
FROM spark_catalog.db.orders WHERE dt = '2026-03-23'
UNION ALL
SELECT 'iceberg' as source, SUM(order_total) as total, COUNT(DISTINCT customer_id) as customers
FROM iceberg_catalog.db.orders
  WHERE order_time >= '2026-03-23' AND order_time < '2026-03-24'
```

**Exit criteria for Phase 2:**
- Shadow writes running for ≥2 weeks without discrepancies
- Row counts match within 0.01% (accounting for timing differences)
- Checksums match exactly
- No ETL job failures related to dual-write

---

### Phase 3: Write Old + New, Read New (Cutover Reads)

**Goal:** Switch downstream consumers to read from Iceberg tables. Hive writes continue
as safety net. If Iceberg reads fail, consumers can fall back to Hive.

**Duration:** ~2 weeks

```
┌────────────────────────────────────────────────────────────────────────┐
│  PHASE 3: READ CUTOVER                                                  │
│                                                                         │
│  Step 3A: Configure read path switching                                │
│  ─────────────────────────────────────                                 │
│  Option 1: View-based abstraction (recommended)                        │
│    CREATE VIEW analytics.orders AS                                     │
│      SELECT * FROM iceberg_catalog.db.orders;  -- switch here         │
│                                                                         │
│    Downstream queries use the VIEW — no code changes needed.           │
│    To rollback: ALTER VIEW analytics.orders AS                         │
│      SELECT * FROM spark_catalog.db.orders;                            │
│                                                                         │
│  Option 2: Catalog namespace aliasing                                  │
│    Configure Trino/Spark to resolve 'db.orders' to Iceberg catalog.   │
│    Trino: change catalog name in connection config.                    │
│    Spark: change default catalog via spark.sql.catalog.defaultCatalog  │
│                                                                         │
│  Option 3: Feature flag per consumer                                   │
│    Each consumer reads a config flag: USE_ICEBERG=true/false           │
│    Gradually roll out: 10% → 50% → 100%                              │
│                                                                         │
│  Step 3B: Performance validation                                       │
│  ───────────────────────────────                                       │
│  Compare query performance on Iceberg vs Hive:                         │
│    - Iceberg benefits: file pruning via sort order, hidden partitions  │
│    - Iceberg overhead: metadata parsing (first query may be slower)    │
│    - Expected: 2-10x improvement for selective queries with sort order │
│    - Expected: similar performance for full-scan aggregations          │
│                                                                         │
│  Step 3C: Monitor and validate continuously                            │
│  ──────────────────────────────────────────                            │
│  - Dashboard: compare Hive vs Iceberg query latencies side-by-side    │
│  - Alert: if Iceberg query latency > 2x Hive latency → investigate   │
│  - Alert: if row count drift > 0.01% → pause and investigate          │
│  - Dual writes STILL RUNNING → Hive data is always available          │
│                                                                         │
│  Step 3D: Rollback procedure                                           │
│  ────────────────────────────                                          │
│  If Iceberg reads have issues:                                         │
│    1. Switch views/configs back to Hive (< 1 minute)                  │
│    2. Investigate Iceberg issue                                        │
│    3. Fix and retry cutover                                            │
│    4. Hive writes never stopped → no data loss                        │
└────────────────────────────────────────────────────────────────────────┘
```

**Exit criteria for Phase 3:**
- All downstream consumers reading from Iceberg for ≥1 week
- No rollbacks triggered
- Query latency within acceptable range (ideally improved)
- Data consistency validated continuously

---

### Phase 4: Write New, Read New (Decommission Old)

**Goal:** Stop writing to Hive tables. Iceberg is now the sole system of record.
Decommission Hive tables and clean up.

**Duration:** ~2 weeks

```
┌────────────────────────────────────────────────────────────────────────┐
│  PHASE 4: DECOMMISSION OLD SYSTEM                                       │
│                                                                         │
│  Step 4A: Stop dual writes                                             │
│  ─────────────────────────                                             │
│  Remove Hive write path from ETL jobs.                                 │
│  Iceberg is now the sole write target.                                 │
│                                                                         │
│  For Tier-3 tables not yet migrated, use in-place migrate:             │
│  CALL catalog.system.migrate(                                          │
│    table => 'spark_catalog.db.small_table',                            │
│    properties => map('format-version', '2')                            │
│  )                                                                     │
│  → Renames Hive table to small_table_BACKUP_                          │
│  → Creates Iceberg table at same location with same name              │
│  → Downstream queries using spark_catalog.db.small_table now          │
│    transparently read Iceberg (if catalog supports it)                │
│                                                                         │
│  Step 4B: Enable Iceberg-specific features                             │
│  ─────────────────────────────────────────                             │
│  Now that Iceberg is the sole writer:                                  │
│    1. Set partition layout:                                            │
│       ALTER TABLE orders ADD PARTITION FIELD hours(order_time)         │
│    2. Set sort order:                                                  │
│       ALTER TABLE orders WRITE ORDERED BY region, customer_id         │
│    3. Run initial sort compaction:                                     │
│       CALL system.rewrite_data_files(table=>'orders', strategy=>'sort')│
│    4. Enable GC (was disabled by snapshot_table):                      │
│       ALTER TABLE orders SET TBLPROPERTIES ('gc.enabled' = 'true')    │
│    5. Set up maintenance jobs:                                         │
│       - Hourly/daily compaction                                       │
│       - Daily snapshot expiration                                     │
│       - Weekly orphan file removal                                    │
│       - Weekly manifest rewriting                                     │
│                                                                         │
│  Step 4C: Clean up Hive tables                                         │
│  ────────────────────────────                                          │
│  ⚠ CAREFUL: Iceberg tables from snapshot_table still reference        │
│  the original Hive Parquet files! Do NOT delete Hive data files       │
│  until compaction has rewritten all files into Iceberg-managed paths.  │
│                                                                         │
│  Safe cleanup sequence:                                                │
│  1. Run full compaction (rewrites all files to Iceberg-managed paths) │
│     CALL system.rewrite_data_files(table => 'orders', strategy => 'sort')
│  2. Verify no files reference old Hive paths:                         │
│     SELECT file_path FROM iceberg_catalog.db.orders.files             │
│       WHERE file_path LIKE '%/hive/warehouse/%'                       │
│     → Should return 0 rows                                            │
│  3. Expire old snapshots that reference Hive files:                   │
│     CALL system.expire_snapshots(table => 'orders', ...)              │
│  4. Remove orphan files (cleans up old Hive references):              │
│     CALL system.remove_orphan_files(table => 'orders', ...)           │
│  5. NOW safe to drop Hive table and delete old Hive data files        │
│  6. Drop migrate backups:                                             │
│     DROP TABLE spark_catalog.db.small_table_BACKUP_                   │
│                                                                         │
│  Step 4D: Decommission Hive Metastore (optional)                      │
│  ────────────────────────────────────────────────                      │
│  If all tables migrated to Iceberg catalog:                            │
│  - Remove Hive Metastore dependency from Spark/Trino configs          │
│  - Shut down Hive Metastore service                                   │
│  - Archive HMS database for audit trail                               │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 5. API Decision Matrix

Which Iceberg API to use at each phase:

| Phase | Action | API | When to Use |
|---|---|---|---|
| 2A | Bootstrap historical data | **`snapshot_table`** | Primary method — creates Iceberg table referencing existing Hive files without modifying source |
| 2A | Bootstrap (non-catalog source) | **`add_files`** | When source is external Parquet files not registered in Hive Metastore |
| 2B | Ongoing dual-write | **App-level dual write** | Best for batch ETL — write same DataFrame to both Hive and Iceberg |
| 2B | Ongoing dual-write (streaming) | **Kafka consumer fanout** | Two consumer groups writing to Hive and Iceberg respectively |
| 2B | Ongoing sync (low-effort) | **`add_files` per partition** | Quick win — import new Hive partitions into Iceberg after each ETL run |
| 4A | In-place migration (Tier-3) | **`migrate`** | Small, low-risk tables where brief unavailability is acceptable |
| 4B | Register pre-existing metadata | **`register_table`** | When Iceberg metadata already exists (e.g., from RewriteTablePath) |
| 4C | Rewrite shared files | **`rewrite_data_files`** | Break dependency on old Hive file paths before deleting Hive data |

**Critical insight: None of these APIs copy data files.** They only create metadata
references to existing Parquet files. This is why `snapshot_table` is nearly instantaneous
even for 30 TB tables — it just reads file listings and Parquet footers.

---

## 6. Risk Mitigation

### The Shared Files Problem

After `snapshot_table`, both Hive and Iceberg reference the **same physical Parquet files**.
This creates a critical dependency:

```
                    ┌──────────────────────┐
                    │  Parquet Files on S3  │
                    │  /warehouse/db/orders │
                    └──────────┬───────────┘
                               │
                    ┌──────────┴───────────┐
                    │                      │
            ┌───────▼──────┐      ┌────────▼─────┐
            │ Hive Table   │      │ Iceberg Table │
            │ (Metastore)  │      │ (Catalog)     │
            └──────────────┘      └──────────────┘

  ⚠ Deleting Hive table data BREAKS Iceberg table!
  ⚠ Hive lifecycle rules (auto-drop partitions) can delete shared files!
  ⚠ Iceberg compaction creates NEW files but old shared files remain referenced
```

**Mitigations:**
1. `snapshot_table` sets `gc.enabled=false` → prevents Iceberg from deleting shared files
2. Disable Hive partition auto-cleanup during migration
3. In Phase 4, run full compaction to rewrite all files to Iceberg-managed paths before deleting Hive data
4. Verify zero Hive-path references before dropping Hive tables

### Dual-Write Consistency

Dual writes can diverge if one write succeeds and the other fails:

```
  ETL Job writes to Hive    → SUCCESS
  ETL Job writes to Iceberg  → FAILS (e.g., schema mismatch, conflict)

  Result: Hive has data that Iceberg doesn't → drift
```

**Mitigations:**
1. Hive write is the primary (production) — it always runs first
2. Iceberg write failure should alert but NOT fail the ETL job (shadow mode)
3. Validation framework catches drift within the next check cycle
4. Automated reconciliation: if Iceberg is behind, use `add_files` to import missing partitions from Hive

---

## 7. Schema & Partition Evolution During Migration

### Partition Redesign (Phase 2 Opportunity)

The migration is the best time to improve partitioning. Old Hive-style partitions often
have issues (over-partitioning, string-typed date columns):

```
Old Hive:    PARTITIONED BY (dt STRING, region STRING)
             → dt='2026-03-23', region='NA'
             → String-based, visible to users, 365 × 4 = 1,460 partitions/year

New Iceberg: PARTITIONED BY (days(order_time), region)
             → Hidden partition on TIMESTAMP column
             → Users query: WHERE order_time > '2026-03-23' (no dt knowledge needed)
             → Iceberg auto-prunes partitions
```

**How to handle the transition:**
- `snapshot_table` preserves original Hive partition layout in Iceberg
- After snapshot, **evolve the partition spec**:
  ```sql
  ALTER TABLE iceberg_catalog.db.orders ADD PARTITION FIELD days(order_time)
  ALTER TABLE iceberg_catalog.db.orders DROP PARTITION FIELD dt
  ALTER TABLE iceberg_catalog.db.orders DROP PARTITION FIELD region
  ALTER TABLE iceberg_catalog.db.orders ADD PARTITION FIELD region
  ```
- Old files: still use old partition spec (Hive-style `dt=` directories)
- New files (from dual-write): use new partition spec (hidden `days(order_time)`)
- Iceberg handles mixed specs transparently at query time
- After Phase 4 compaction: all files use new partition spec

### Sort Order Addition

Add sort order after migration (not possible with Hive):
```sql
ALTER TABLE iceberg_catalog.db.orders WRITE ORDERED BY region, customer_id
```
- Only affects new writes (existing files remain unsorted)
- Full sort compaction in Phase 4 applies sort order to all data
- Dramatically improves read performance for selective queries

---

## 8. Further Improvements

**Additional questions for deeper discussion:**
- How to handle Hive UDFs that don't exist in Iceberg/Trino?
- How to migrate Hive ACID tables (managed tables with transactions)?
- What about Hive views that depend on migrated tables?
- How to handle cross-table consistency (migrate related tables together)?
- Cost analysis: Hive Metastore infra vs Iceberg catalog infra?

**Advanced topics:**
- **Automated migration orchestration**: Airflow DAG that runs snapshot_table → validation → dual-write setup → Phase 3 cutover for each table automatically
- **Canary-based rollout**: Route 5% of reads to Iceberg first, monitor, then gradually increase
- **Nessie branches for safe migration**: Create a branch, test migration on branch, merge to main if successful
- **Data quality gates**: Automated checks that must pass before advancing to next phase (Great Expectations, Deequ)

---

## Appendix: Spark + Iceberg Code Examples

### A1. Phase 1: Baseline metrics collection

```python
# Collect baseline metrics for all Hive tables
from pyspark.sql.functions import count, sum as spark_sum, countDistinct

tables = ["db.orders", "db.customers", "db.products", "db.inventory"]

for table_name in tables:
    df = spark.table(f"spark_catalog.{table_name}")
    metrics = df.agg(
        count("*").alias("row_count"),
        spark_sum("order_total").alias("total_sum") if "order_total" in df.columns else lit(None),
        countDistinct("customer_id").alias("distinct_customers") if "customer_id" in df.columns else lit(None)
    ).collect()[0]
    print(f"{table_name}: rows={metrics.row_count}, sum={metrics.total_sum}, customers={metrics.distinct_customers}")
```

### A2. Phase 2A: Bootstrap with snapshot_table

```python
# Snapshot a Hive table into Iceberg (no data copy)
spark.sql("""
CALL iceberg_catalog.system.snapshot(
    source_table => 'spark_catalog.db.orders',
    table => 'iceberg_catalog.db.orders',
    properties => map(
        'format-version', '2',
        'write.parquet.compression-codec', 'zstd'
    )
)
""")

# Verify: both tables should have identical row counts
hive_count = spark.table("spark_catalog.db.orders").count()
ice_count = spark.table("iceberg_catalog.db.orders").count()
assert hive_count == ice_count, f"Row count mismatch: Hive={hive_count}, Iceberg={ice_count}"
print(f"Snapshot successful: {ice_count} rows")
```

### A3. Phase 2A-alt: Bootstrap with add_files (external Parquet)

```python
# Create Iceberg table with desired schema
spark.sql("""
CREATE TABLE iceberg_catalog.db.orders (
    order_id BIGINT, customer_id BIGINT, order_total DECIMAL(12,2),
    region STRING, order_time TIMESTAMP
) USING iceberg
PARTITIONED BY (days(order_time), region)
TBLPROPERTIES ('format-version' = '2')
""")

# Import existing Parquet files (no copy — just metadata references)
spark.sql("""
CALL iceberg_catalog.system.add_files(
    table => 'db.orders',
    source_table => '`parquet`.`s3://warehouse/db/orders`',
    parallelism => 8
)
""")
```

### A4. Phase 2B: Dual-write ETL job

```python
# Modified ETL job: dual-write to Hive and Iceberg
df = spark.read.parquet("s3://raw/orders/2026-03-23/") \
    .transform(clean_and_enrich)  # existing transformations

# Write 1: Hive (production — must succeed)
df.write.mode("append").insertInto("spark_catalog.db.orders")

# Write 2: Iceberg (shadow — failure is non-fatal)
try:
    df.writeTo("iceberg_catalog.db.orders").append()
except Exception as e:
    alert(f"Shadow Iceberg write failed: {e}")
    # Log but don't fail the job — Hive write already succeeded
```

### A5. Phase 2B-alt: Sync via add_files (after Hive write)

```python
# After Hive ETL completes, import the new partition into Iceberg
spark.sql("""
CALL iceberg_catalog.system.add_files(
    table => 'db.orders',
    source_table => 'spark_catalog.db.orders',
    partition_filter => map('dt', '2026-03-23')
)
""")
```

### A6. Phase 2: Validation framework

```python
# Automated validation: compare Hive vs Iceberg
def validate_table(table_name, partition_filter):
    hive_df = spark.table(f"spark_catalog.{table_name}").filter(partition_filter)
    ice_df = spark.table(f"iceberg_catalog.{table_name}").filter(partition_filter)

    hive_count = hive_df.count()
    ice_count = ice_df.count()

    if hive_count != ice_count:
        alert(f"ROW COUNT MISMATCH {table_name}: Hive={hive_count}, Iceberg={ice_count}")
        return False

    # Column-level checksum
    for col in ["order_total", "quantity"]:
        if col in hive_df.columns:
            hive_sum = hive_df.agg(spark_sum(col)).collect()[0][0]
            ice_sum = ice_df.agg(spark_sum(col)).collect()[0][0]
            if abs(hive_sum - ice_sum) > 0.01:
                alert(f"CHECKSUM MISMATCH {table_name}.{col}: Hive={hive_sum}, Iceberg={ice_sum}")
                return False

    print(f"VALIDATED {table_name}: {hive_count} rows, checksums match")
    return True

validate_table("db.orders", "dt = '2026-03-23'")
```

### A7. Phase 3: View-based read switching

```sql
-- Create abstraction view (consumers query this)
CREATE OR REPLACE VIEW analytics.orders AS
SELECT * FROM spark_catalog.db.orders;  -- initially points to Hive

-- Phase 3 cutover: switch to Iceberg
CREATE OR REPLACE VIEW analytics.orders AS
SELECT * FROM iceberg_catalog.db.orders;

-- Rollback if needed: switch back to Hive
CREATE OR REPLACE VIEW analytics.orders AS
SELECT * FROM spark_catalog.db.orders;
```

### A8. Phase 4: In-place migrate for Tier-3 tables

```python
# In-place migration (renames Hive table to backup, creates Iceberg at same name)
spark.sql("""
CALL iceberg_catalog.system.migrate(
    table => 'spark_catalog.db.small_lookup_table',
    properties => map('format-version', '2'),
    drop_backup => false
)
""")

# Verify migration
print(spark.table("spark_catalog.db.small_lookup_table").count())

# Later, after validation, drop backup:
spark.sql("DROP TABLE spark_catalog.db.small_lookup_table_BACKUP_")
```

### A9. Phase 4: Compaction to break Hive file dependency

```python
# Rewrite ALL files to Iceberg-managed paths (breaks dependency on Hive paths)
spark.sql("""
CALL iceberg_catalog.system.rewrite_data_files(
    table => 'db.orders',
    strategy => 'sort',
    sort_order => 'days(order_time) ASC, region ASC, customer_id ASC',
    options => map('target-file-size-bytes', '268435456')
)
""")

# Verify no files still reference old Hive paths
stale_files = spark.sql("""
SELECT file_path FROM iceberg_catalog.db.orders.all_data_files
WHERE file_path LIKE '%/hive/warehouse/%' OR file_path LIKE '%/dt=%'
""")

if stale_files.count() == 0:
    print("All files rewritten to Iceberg-managed paths. Safe to delete Hive data.")
else:
    print(f"WARNING: {stale_files.count()} files still reference Hive paths!")
    stale_files.show(truncate=False)
```

### A10. Phase 4: Enable Iceberg features and maintenance

```python
# Enable GC (was disabled by snapshot_table)
spark.sql("ALTER TABLE iceberg_catalog.db.orders SET TBLPROPERTIES ('gc.enabled' = 'true')")

# Evolve partitions to hidden partitions
spark.sql("ALTER TABLE iceberg_catalog.db.orders ADD PARTITION FIELD days(order_time)")
# Note: old Hive partitions (dt=) still valid for historical files

# Set sort order
spark.sql("ALTER TABLE iceberg_catalog.db.orders WRITE ORDERED BY region, customer_id")

# Expire snapshots (including old snapshot_table-era snapshots)
spark.sql("""
CALL iceberg_catalog.system.expire_snapshots(
    table => 'db.orders',
    older_than => TIMESTAMP '2026-03-16 00:00:00',
    retain_last => 50
)
""")

# Remove orphan files (cleans up old Hive file references after compaction)
spark.sql("""
CALL iceberg_catalog.system.remove_orphan_files(
    table => 'db.orders',
    older_than => TIMESTAMP '2026-03-16 00:00:00'
)
""")

# Rewrite manifests for optimal query planning
spark.sql("CALL iceberg_catalog.system.rewrite_manifests(table => 'db.orders')")
```
