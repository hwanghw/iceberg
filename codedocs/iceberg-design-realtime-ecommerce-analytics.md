# Design Problem: Real-Time E-Commerce Order Analytics Platform

Design a real-time analytics platform for an e-commerce company processing millions of orders per day. The system must provide near real-time dashboards for business teams while maintaining a queryable historical data lake.

---

## 1. Requirements Gathering (Key Questions)

| # | Question | Expected Answer | Design Impact |
|---|----------|----------------|---------------|
| 1 | What's the **order event rate**? Peak vs sustained? | 50K events/sec peak, 20K sustained | Kafka partitions, Flink parallelism, commit frequency |
| 2 | What's the **end-to-end latency SLA**? | Data visible in dashboards within 5 minutes | Determines commit interval (1-5 min), compaction urgency |
| 3 | What are the **primary query patterns**? | "Revenue by region/category in last 24h", "order status by customer", daily GMV | Drives partition (time) + sort order (region, category) |
| 4 | Are there **updates** to orders? (status changes, cancellations) | Yes — order status changes (placed→shipped→delivered→returned) | COW vs MOR decision; ~5% of records updated within 48h |
| 5 | What's the **data retention**? | Hot: 90 days queryable, Cold: 3 years archived | Snapshot expiration policy, storage tiering |
| 6 | What **engines** read the data? | Trino for dashboards, Spark for batch aggregation, ML training | Multi-engine catalog (REST/Glue), consistent metadata |
| 7 | Is the schema **stable or evolving**? | New fields added quarterly (e.g., loyalty_tier, promo_code) | Schema evolution without rewriting existing data |

---

## 2. Capacity Estimation

```
Given:
  peak_events_sec      = 50,000
  avg_event_size       = 800 bytes (JSON) → ~400 bytes (Parquet compressed)
  sustained_events_sec = 20,000
  daily_orders         = 20,000 × 86,400 = ~1.7 billion events/day

Derived:
  daily_ingest_raw     = 1.7B × 800 B = ~1.4 TB/day (raw JSON)
  daily_ingest_parquet = 1.7B × 400 B = ~680 GB/day (Parquet)
  yearly_data          = 680 GB × 365 = ~245 TB/year

File sizing:
  target_file_size     = 256 MB
  files_per_day        = 680 GB / 256 MB = ~2,660 files/day

Partition sizing (partition by hour):
  partition_size       = 680 GB / 24 = ~28 GB/hour
  files_per_partition  = 28 GB / 256 MB = ~110 files (post-compaction)

Streaming small files (pre-compaction):
  commit_interval      = 2 minutes
  flink_parallelism    = 64 writers
  files_per_commit     = 64
  files_per_hour       = 64 × 30 = 1,920 small files/hour
  avg_small_file_size  = 28 GB / 1,920 = ~15 MB each  ← MUST compact

Kafka:
  partitions           = 128 (for 50K/sec at ~400 events/sec/partition)
  retention            = 7 days (replay buffer)

Metadata:
  snapshots/day        = 24 × 30 = 720 (every 2 min)
  snapshot_retention   = 3 days = ~2,160 active snapshots
  → Must expire aggressively
```

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    REAL-TIME E-COMMERCE ANALYTICS                         │
│                                                                          │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐    ┌──────────┐  │
│  │ Order    │    │  Kafka   │    │  Flink Streaming  │    │ Iceberg  │  │
│  │ Service  │───▶│ 128 parts│───▶│  64 parallelism   │───▶│ Tables   │  │
│  │ (events) │    │ 7d retain│    │  2-min checkpoint  │    │ on S3    │  │
│  └──────────┘    └──────────┘    └──────────────────┘    └────┬─────┘  │
│                                                                │        │
│  ┌──────────┐                    ┌──────────────────┐    ┌────▼─────┐  │
│  │ Catalog  │◀───────────────────│  Compaction Svc   │    │ Query    │  │
│  │ (REST/   │                    │  (Spark, hourly)  │───▶│ Engines  │  │
│  │  Glue)   │                    │  binpack + sort   │    │ Trino/   │  │
│  └──────────┘                    └──────────────────┘    │ Spark    │  │
│                                                           └────┬─────┘  │
│                                  ┌──────────────────┐    ┌────▼─────┐  │
│                                  │  Maintenance      │    │Dashboards│  │
│                                  │  (Airflow DAG)    │    │  Superset│  │
│                                  │  expire/orphan/   │    │  Grafana │  │
│                                  │  rewrite manifests│    └──────────┘  │
│                                  └──────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘

Data Flow:
  Order placed → Kafka topic "orders" → Flink consumes, transforms, enriches
  → Flink writes to Iceberg every 2 min (checkpoint commit)
  → Compaction service rewrites small files hourly
  → Trino queries compacted data for dashboards
  → Spark runs daily batch aggregations
```

---

## 4. Deep Dive

### 4.1 Schema Design

```sql
CREATE TABLE catalog.ecommerce.orders (
    order_id        BIGINT,
    customer_id     BIGINT,
    order_status    STRING,      -- placed, confirmed, shipped, delivered, returned, cancelled
    order_total     DECIMAL(12,2),
    currency        STRING,
    item_count      INT,
    category        STRING,      -- electronics, clothing, home, etc.
    region          STRING,      -- NA, EU, APAC, LATAM
    country         STRING,
    channel         STRING,      -- web, mobile_app, in_store
    payment_method  STRING,      -- credit_card, paypal, bank_transfer
    event_time      TIMESTAMP,   -- when the event occurred
    processing_time TIMESTAMP,   -- when Flink processed it
    updated_at      TIMESTAMP    -- last status change time
) USING iceberg
PARTITIONED BY (hours(event_time), region)
```

**Why this schema:**
- `order_id` as natural key for dedup and merge operations
- `event_time` vs `processing_time` separation for event-time semantics
- `order_status` enables tracking lifecycle (critical for COW vs MOR decision)
- `region` as top-level dimension for most business queries

### 4.2 Partition Design

```
PARTITIONED BY (hours(event_time), region)
```

**Analysis:**
- **`hours(event_time)`**: Hidden partition. At 28 GB/hour and 4 regions, each partition = ~7 GB. Within sweet spot (1-10 GB).
- **`region`**: 4 values (NA, EU, APAC, LATAM). Low cardinality — good. Most dashboards filter by region.
- **Total partitions/day**: 24 hours × 4 regions = 96 partitions/day
- **Files per partition** (post-compaction): 7 GB / 256 MB = ~27 files ← excellent

**Why NOT partition by `category`?**
- 20+ categories × 24 hours × 4 regions = 1,920+ partitions/day → too many
- Many partitions would be < 100 MB → over-partitioned
- Instead, use `category` in the sort order for file-level pruning

### 4.3 Sort Order Design

```sql
ALTER TABLE catalog.ecommerce.orders WRITE ORDERED BY category, customer_id
```

**Effective sort order** (after `SortOrderUtil.buildSortOrder()` prepends partitions):
```
[hours(event_time) ASC, region ASC, category ASC, customer_id ASC]
```

**Why this sort order:**
- Partition transforms `hours(event_time)` and `region` are auto-prepended
- `category` sorts within each partition → tight min/max ranges per file → queries like `WHERE category = 'electronics'` skip 80%+ of files
- `customer_id` as secondary sort → point lookups for customer order history benefit from file pruning

### 4.4 Write Distribution Mode

```
write.distribution-mode = range   (set implicitly by WRITE ORDERED BY)
```

**Analysis:**
- `WRITE ORDERED BY` sets `write.distribution-mode=range` via the DDL parser
- Spark performs a **global range shuffle on the new incoming data** before writing. Only the
  records being written in the current operation are shuffled — existing files on S3 are never
  touched. The shuffle happens in Spark's in-memory execution plan for the current write only.
- Produces files with **non-overlapping value ranges** for `[hours(event_time), region, category, customer_id]`
- **Trade-off**: More expensive shuffle at write time, but dramatically better read pruning

**RANGE vs HASH — what "local sort" means:**

```
RANGE distribution (WRITE ORDERED BY category, customer_id):
  Global range shuffle across ALL tasks for the new batch of records.
  Spark coordinates which value ranges go to which task:

  Task 1 gets: category=clothing,    customer_id=1-1000     → writes file_1
  Task 2 gets: category=clothing,    customer_id=1001-2000  → writes file_2
  Task 3 gets: category=electronics, customer_id=1-500      → writes file_3

  Files have NON-OVERLAPPING ranges: file_1 and file_3 don't overlap on category.
  Query WHERE category='electronics' skips file_1 and file_2 via min/max stats.

HASH distribution (WRITE DISTRIBUTED BY PARTITION ORDERED BY category, customer_id):
  Hash shuffle by partition columns only (hours, region).
  Then each task INDEPENDENTLY sorts its own records — no cross-task coordination.

  Task 1 (hours=14, region=NA): sorted locally → clothing/1, clothing/50, electronics/3
  Task 2 (hours=14, region=EU): sorted locally → clothing/10, electronics/5, electronics/150

  BOTH tasks' files contain "clothing" → OVERLAPPING ranges across files!
  Query WHERE category='clothing' cannot skip either file — must scan both.
```

"Local sort" means within each Spark task (each shuffle partition), NOT across tasks. After
the hash shuffle assigns records to tasks by partition key, each task sorts its own batch
independently via Spark's `SortExec` operator. There's no coordination between tasks about
which value ranges each gets, so files from different tasks can overlap on the sort columns.

**Why not HASH for this workload?**
- Hash would cluster by partition (hours, region) — correct for partition pruning
- But sort on (category, customer_id) would be local per task — files overlap on category
- For dashboards filtering by category or customer, RANGE gives 80%+ file pruning; HASH gives ~0%
- For this read-heavy workload, RANGE is justified despite the more expensive shuffle

**Streaming consideration (Flink):**

Flink's Iceberg sink does NOT use Spark's `SparkWriteConf` or `write.distribution-mode`. Each Flink
writer task writes records as they arrive from Kafka partitions — no range shuffle, no hash shuffle.
The `write.distribution-mode=range` setting is **effectively ignored by Flink**.

```
Flink streaming write (every 2-min checkpoint):
  64 writer tasks, each writing ~15 MB file
  Row order = whatever Kafka partition order delivered
  No shuffle, no global sort, no local sort by table sort order
  Files have wide, overlapping value ranges → no min/max pruning benefit
  write.distribution-mode = irrelevant for Flink
```

For Flink streaming tables, `WRITE ORDERED BY` is effectively a **compaction sort order declaration**
— it sets `table.sortOrder()` which compaction reads, but Flink itself never uses it at write time.

**How compaction uses the sort order:**

Compaction `strategy=sort` auto-reads `table.sortOrder()` (set by `WRITE ORDERED BY`). You do
NOT need to re-specify the sort order or distribution mode during compaction:

```
SparkSortFileRewriteRunner (line 39):
  this.sortOrder = table.sortOrder();    // reads table's sort order automatically

SparkShufflingFileRewriteRunner (line 120):
  .option(USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")  // disables table's normal write path
  // Instead builds its own RANGE sort plan internally
```

Compaction `strategy=binpack` does NOT do a global sort, but there is a subtlety:

```
SparkBinPackFileRewriteRunner:
  .option(DISTRIBUTION_MODE, "none")     // no shuffle
  // Does NOT set USE_TABLE_DISTRIBUTION_AND_ORDERING = false
  // Default is true → Spark V2 write path sees table's requiredOrdering()
  // → adds Sort(global=false) → LOCAL sort within each task

Result: binpack output files ARE locally sorted within each file by the table's
sort order, but files from different tasks have OVERLAPPING value ranges
(no cross-task coordination). This is because Spark's V2 RequiresDistributionAndOrdering
interface returns the ordering, and Spark adds a local SortExec.
```

**Corrected compaction strategy comparison:**

| Strategy | Global shuffle? | Sort within each file? | Non-overlapping across files? | Source |
|---|---|---|---|---|
| **binpack** | NO | YES (local, via Spark V2 requiredOrdering) | NO — files overlap | `BinPackFileRewriteRunner`: DISTRIBUTION_MODE=NONE, but table ordering still applies |
| **sort** | YES (RANGE) | YES (global sort) | YES — non-overlapping ranges | `SortFileRewriteRunner`: own sort plan, USE_TABLE_DISTRIBUTION_AND_ORDERING=false |
| **zorder** | YES (RANGE) | YES (z-order) | YES (multi-dimensional) | `ZOrderFileRewriteRunner`: own z-order plan |

### 4.5 Read/Write Performance Analysis

**Write path:**
```
Flink → checkpoint every 2 min → 64 writers × 1 file each = 64 files/commit
  Each file: ~15 MB (small, unsorted)
  Write throughput: ~28 GB/hour sustained
  Distribution mode during streaming: effectively NONE (Flink doesn't do Spark-style shuffle)
  Sort order during streaming: local sort within each Flink writer task only
```

**Read path (pre-compaction):**
```
Query: SELECT SUM(order_total) FROM orders WHERE hours(event_time) = X AND region = 'NA'
  Partitions scanned: 1 (time-partition pruned)
  Files in partition: ~480 small files (hourly accumulation before compaction)
  File pruning via min/max: minimal (unsorted files have wide value ranges)
  → Planning overhead: HIGH (many small files)
  → Scan volume: reads all files in partition
```

**Read path (post-compaction with sort):**
```
Same query after hourly sort compaction:
  Partitions scanned: 1
  Files in partition: ~27 well-sized files (256 MB each, globally sorted)
  File pruning: if filtering on category, min/max stats skip ~80% of files
  → Planning overhead: LOW (few files)
  → Scan volume: ~5 files instead of 480
  → 96x improvement in planning, ~90% reduction in data scanned for selective queries
```

### 4.6 Maintenance Jobs

```
┌────────────────────────────────────────────────────────────────────────┐
│  MAINTENANCE SCHEDULE (Airflow DAG)                                     │
│                                                                         │
│  EVERY HOUR: Compaction (most critical for streaming tables)           │
│    CALL catalog.system.rewrite_data_files(                             │
│      table => 'ecommerce.orders',                                      │
│      strategy => 'sort',                                               │
│      sort_order => 'hours(event_time) ASC, region ASC,                 │
│                     category ASC, customer_id ASC',                    │
│      options => map(                                                   │
│        'target-file-size-bytes', '268435456',   -- 256 MB              │
│        'min-file-size-bytes',    '67108864',    -- 64 MB               │
│        'max-file-size-bytes',    '536870912',   -- 512 MB              │
│        'partial-progress.enabled', 'true'                              │
│      )                                                                 │
│    )                                                                   │
│    Focus: compact only recent partitions (last 2 hours)                │
│    I/O cost: ~56 GB read + 56 GB write per run                        │
│                                                                         │
│  EVERY 6 HOURS: Expire snapshots                                       │
│    CALL catalog.system.expire_snapshots(                               │
│      table => 'ecommerce.orders',                                      │
│      older_than => TIMESTAMP 'now - 3 days',                           │
│      retain_last => 100                                                │
│    )                                                                   │
│    Why: 720 snapshots/day from 2-min commits → metadata bloat          │
│                                                                         │
│  DAILY: Remove orphan files                                            │
│    CALL catalog.system.remove_orphan_files(                            │
│      table => 'ecommerce.orders',                                      │
│      older_than => TIMESTAMP 'now - 5 days'                            │
│    )                                                                   │
│    Why: failed commits leave orphaned data files on S3                 │
│                                                                         │
│  WEEKLY: Rewrite manifests                                             │
│    CALL catalog.system.rewrite_manifests(                              │
│      table => 'ecommerce.orders'                                       │
│    )                                                                   │
│    Why: many small manifests accumulate from frequent commits          │
│    Rewriting consolidates them → faster query planning                 │
│                                                                         │
│  MONTHLY: Full sort compaction on historical partitions                │
│    Rewrite older partitions (30-90 days) with full sort + zorder       │
│    for ad-hoc analytical queries on historical data                    │
└────────────────────────────────────────────────────────────────────────┘
```

### 4.7 Schema Evolution & Partition Evolution Impact

**Schema Evolution scenarios:**
- **Adding `loyalty_tier` column**: `ALTER TABLE orders ADD COLUMN loyalty_tier STRING`
  - Old Parquet files: return `null` for `loyalty_tier` — no rewrite needed
  - New files: include the column
  - Downstream dashboards: must handle `null` gracefully (COALESCE)
  - ML pipelines: time-travel to pre-evolution snapshots for reproducibility (as shown in the PDF's TelCan use case)

- **Adding `promo_code` column mid-stream**: Flink job must be updated to populate the new field
  - Iceberg tracks schema by field ID, not position → safe even if Flink produces records with different column counts
  - No rewrite of existing data needed

- **Renaming `channel` to `sales_channel`**: safe — tracked by field ID, not name. Old code using `channel` must be updated, but old data files remain readable.

**Partition Evolution scenarios:**
- **Initial**: `PARTITIONED BY (hours(event_time), region)`
- **After 1 year, data grows 3x**: partitions become 21 GB → borderline
  - Evolve: `ALTER TABLE orders ADD PARTITION FIELD bucket(4, category)`
  - New files: partitioned by `[hours(event_time), region, bucket(4, category)]`
  - Old files: still `[hours(event_time), region]` — Iceberg handles mixed specs transparently
  - Partition size: 21 GB / 4 buckets = ~5 GB ← back to sweet spot
  - **Compaction caveat**: don't mix files with different partition specs in same compaction job

---

## 5. Further Improvements

**Additional requirement questions for deeper discussion:**
- What's the deduplication strategy? (Kafka at-least-once can produce duplicates)
- Are there multi-stream joins? (orders + payments + shipments)
- What alerting is needed? (anomaly detection on order rates)
- Is there a need for real-time aggregation (pre-computed materialized views)?
- What's the cost budget? (S3 storage tiers, compute instance types)

**Advanced topics:**
- **Exactly-once guarantees**: Flink checkpoints + Iceberg's atomic commits = end-to-end exactly-once. Iceberg sink tracks `flink.checkpoint-id` in snapshot summary.
- **Incremental reads**: Downstream Flink jobs can use `streaming-read` on Iceberg tables for CDC-style consumption of new snapshots.
- **Z-Order compaction**: For ad-hoc queries on `(category, country, payment_method)`, z-order provides balanced multi-dimensional clustering.
- **Bloom filters**: `write.metadata.metrics.column.customer_id=full` + bloom filter for high-cardinality point lookups.
- **Branch-based A/B testing**: Create branches for experimental dashboard data without affecting production (as shown in PDF's TelCan ML experimentation use case).

---

## Appendix: Spark + Iceberg Code Examples

### A1. Create the orders table

```python
spark.sql("""
CREATE TABLE catalog.ecommerce.orders (
    order_id        BIGINT,
    customer_id     BIGINT,
    order_status    STRING,
    order_total     DECIMAL(12,2),
    currency        STRING,
    item_count      INT,
    category        STRING,
    region          STRING,
    country         STRING,
    channel         STRING,
    payment_method  STRING,
    event_time      TIMESTAMP,
    processing_time TIMESTAMP,
    updated_at      TIMESTAMP
) USING iceberg
PARTITIONED BY (hours(event_time), region)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
""")

-- Set sort order and distribution mode
spark.sql("""
ALTER TABLE catalog.ecommerce.orders
WRITE ORDERED BY category, customer_id
""")
```

### A2. Streaming write (Flink SQL)

```sql
-- Flink SQL: create Iceberg sink table
CREATE TABLE iceberg_orders WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'catalog',
    'catalog-database' = 'ecommerce',
    'catalog-table' = 'orders',
    'catalog-type' = 'rest',
    'uri' = 'http://rest-catalog:8181'
);

-- Insert from Kafka source
INSERT INTO iceberg_orders
SELECT order_id, customer_id, order_status, order_total, currency,
       item_count, category, region, country, channel, payment_method,
       event_time, PROCTIME() as processing_time, event_time as updated_at
FROM kafka_orders;
```

### A3. Compaction (Spark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.catalog.type", "rest") \
    .config("spark.sql.catalog.catalog.uri", "http://rest-catalog:8181") \
    .getOrCreate()

# Sort compaction on recent partitions
spark.sql("""
CALL catalog.system.rewrite_data_files(
    table => 'ecommerce.orders',
    strategy => 'sort',
    sort_order => 'hours(event_time) ASC, region ASC, category ASC, customer_id ASC',
    where => 'event_time >= current_timestamp() - INTERVAL 2 HOURS',
    options => map(
        'target-file-size-bytes', '268435456',
        'partial-progress.enabled', 'true'
    )
)
""")
```

### A4. Expire snapshots and clean up

```python
# Expire old snapshots (keep 3 days)
spark.sql("""
CALL catalog.system.expire_snapshots(
    table => 'ecommerce.orders',
    older_than => TIMESTAMP '2026-03-20 00:00:00',
    retain_last => 100
)
""")

# Remove orphan files
spark.sql("""
CALL catalog.system.remove_orphan_files(
    table => 'ecommerce.orders',
    older_than => TIMESTAMP '2026-03-18 00:00:00'
)
""")

# Rewrite manifests
spark.sql("""
CALL catalog.system.rewrite_manifests(
    table => 'ecommerce.orders'
)
""")
```

### A5. Schema evolution

```python
# Add new column without rewriting data
spark.sql("ALTER TABLE catalog.ecommerce.orders ADD COLUMN loyalty_tier STRING")
spark.sql("ALTER TABLE catalog.ecommerce.orders ADD COLUMN promo_code STRING")

# Time-travel query to pre-evolution data
df_old = spark.read \
    .option("as-of-timestamp", "1711929600000") \
    .format("iceberg") \
    .load("catalog.ecommerce.orders")
```

### A6. Query with partition and file pruning

```python
# This query benefits from partition pruning (hours + region)
# AND file-level pruning (category sort order → min/max stats)
spark.sql("""
SELECT region, category, SUM(order_total) as revenue, COUNT(*) as order_count
FROM catalog.ecommerce.orders
WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
  AND region = 'NA'
  AND category = 'electronics'
GROUP BY region, category
""")
```
