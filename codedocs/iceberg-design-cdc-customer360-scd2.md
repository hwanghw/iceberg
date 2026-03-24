# Design Problem: CDC-Based Customer 360 with SCD Type-2

Design a Customer 360 data platform for a retail company (Nova Trends) that captures changes from multiple OLTP databases via CDC, maintains full history of customer dimension changes using SCD Type-2, and provides a unified view for analytics, marketing, and personalization. Inspired by the Nova Trends use case from "Apache Iceberg: The Definitive Guide."

---

## 1. Requirements Gathering (Key Questions)

| # | Question | Expected Answer | Design Impact |
|---|----------|----------------|---------------|
| 1 | What **source databases** feed the CDC pipeline? | 3 PostgreSQL databases (customers, orders, inventory) | Need Debezium CDC connectors, schema registry for each source |
| 2 | What's the **change volume**? How often do customer records change? | 50M customers total, ~500K changes/day (1% churn), 5M orders/day | Drives COW vs MOR: 1% updates → MOR with position deletes |
| 3 | Which **dimension attributes** change? How to track history? | Address, email, loyalty_tier, phone change; must retain full history | SCD Type-2 with eff_start_date, eff_end_date, is_current flag |
| 4 | What's the **latency requirement** for CDC propagation? | Changes visible within 30 minutes | Micro-batch CDC every 10-15 min, not real-time streaming |
| 5 | What are the **primary query patterns**? | "Current customer profile", "customer history over time", "sales by region at point-in-time" | Current queries filter `is_current=true`; historical need temporal joins |
| 6 | Is there a **natural key** for deduplication? | `customer_id` from source DB (unique, immutable) | MERGE INTO on customer_id; surrogate `customer_dim_key` for fact-dimension join |
| 7 | How is the **fact table** (sales) linked to dimension changes? | Sales must link to the correct customer version at time of sale | Surrogate key (`customer_dim_key`) + temporal join on eff_start_date/eff_end_date |

---

## 2. Capacity Estimation

```
Customer Dimension Table:
  total_customers      = 50,000,000
  avg_record_size      = 500 bytes (Parquet compressed)
  changes_per_day      = 500,000 (new SCD2 rows + expired rows updated)
  SCD2_growth_rate     = 500K new rows/day (old rows updated, new rows inserted)
  avg_versions/customer = 3 (over lifetime)
  total_rows           = 50M × 3 = 150M rows
  table_size           = 150M × 500 B = ~75 GB

  Post-SCD2-merge I/O per run:
    rows_to_merge      = 500K changed + 500K to expire = 1M rows
    files_rewritten    = depends on COW vs MOR
      COW: rewrite all files containing changed rows → ~5% of 75 GB = 3.75 GB
      MOR: write 1M position delete entries = ~10 MB + 500K new rows = ~250 MB
    → MOR is 15x cheaper for writes

Sales Fact Table:
  orders_per_day       = 5,000,000
  avg_record_size      = 300 bytes (Parquet compressed)
  daily_data           = 5M × 300 B = ~1.5 GB/day
  yearly_data          = 1.5 GB × 365 = ~550 GB/year

Partition sizing (customers dimension):
  PARTITIONED BY (bucket(64, customer_id))
  partition_size       = 75 GB / 64 = ~1.2 GB per bucket ← sweet spot
  files_per_partition  = 1.2 GB / 256 MB = ~5 files

Partition sizing (sales fact):
  PARTITIONED BY (days(event_time))
  partition_size       = 1.5 GB/day ← sweet spot
  files_per_partition  = 1.5 GB / 256 MB = ~6 files
```

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                  CDC-BASED CUSTOMER 360 WITH SCD2                        │
│                                                                          │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐                   │
│  │PostgreSQL│    │ Debezium │    │     Kafka         │                   │
│  │ (OLTP)   │───▶│   CDC    │───▶│  CDC Topics       │                   │
│  │customers │    │Connector │    │ (customer_changes, │                  │
│  │orders    │    └──────────┘    │  order_events)    │                   │
│  │inventory │                    └────────┬──────────┘                   │
│  └──────────┘                             │                              │
│                                           ▼                              │
│                              ┌──────────────────────┐                   │
│                              │  Spark Micro-Batch    │                   │
│                              │  (every 15 min)       │                   │
│                              │                       │                   │
│                              │  1. Read CDC events   │                   │
│                              │  2. Dedup by key+ts   │                   │
│                              │  3. Apply SCD2 logic  │                   │
│                              │  4. MERGE INTO Iceberg│                   │
│                              └───────────┬───────────┘                   │
│                                          │                               │
│            ┌─────────────────────────────┼──────────────────┐           │
│            ▼                             ▼                  ▼           │
│  ┌─────────────────┐    ┌─────────────────────┐   ┌──────────────┐    │
│  │ Customers (dim)  │    │    Sales (fact)      │   │  Inventory   │    │
│  │ Iceberg SCD2     │    │    Iceberg           │   │  Iceberg     │    │
│  │                  │    │                      │   │              │    │
│  │ customer_dim_key │◄───│ customer_dim_key(FK) │   │              │    │
│  │ customer_id      │    │ order_id             │   │              │    │
│  │ eff_start_date   │    │ event_time           │   │              │    │
│  │ eff_end_date     │    │ order_total          │   │              │    │
│  │ is_current       │    │ ...                  │   │              │    │
│  └─────────────────┘    └─────────────────────┘   └──────────────┘    │
│            │                         │                                  │
│            ▼                         ▼                                  │
│  ┌──────────────────────────────────────────────┐                      │
│  │  Query Engines: Trino / Spark SQL             │                     │
│  │  - Current customer: WHERE is_current = true  │                     │
│  │  - Point-in-time: temporal join on eff dates  │                     │
│  │  - Sales by region: JOIN on customer_dim_key  │                     │
│  └──────────────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Deep Dive

### 4.1 Schema Design

**Customer Dimension (SCD Type-2):**
```sql
CREATE TABLE catalog.customer360.customers (
    customer_dim_key  BIGINT,     -- surrogate key (unique per version)
    customer_id       BIGINT,     -- natural key from source DB
    first_name        STRING,
    last_name         STRING,
    email             STRING,
    phone             STRING,
    address           STRING,
    city              STRING,
    country           STRING,
    loyalty_tier      STRING,     -- bronze, silver, gold, platinum
    eff_start_date    DATE,       -- when this version became active
    eff_end_date      DATE,       -- when this version was superseded (9999-12-31 if current)
    is_current        BOOLEAN,    -- true for the active version
    source_updated_at TIMESTAMP   -- CDC timestamp from source DB
) USING iceberg
PARTITIONED BY (bucket(64, customer_id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
)
```

**Why this design:**
- `customer_dim_key`: Surrogate key (e.g., timestamp-based UUID) — unique per version of a customer. The Sales fact table uses this to join to the correct historical version.
- `customer_id`: Natural key from source — used in MERGE INTO for matching CDC changes.
- `eff_start_date / eff_end_date / is_current`: Standard SCD2 columns. `eff_end_date = '9999-12-31'` indicates current version.
- `source_updated_at`: CDC timestamp — used to order changes and dedup.

**Sales Fact:**
```sql
CREATE TABLE catalog.customer360.sales (
    order_id          BIGINT,
    customer_id       BIGINT,
    customer_dim_key  BIGINT,     -- FK to customers dimension (specific version)
    item_id           BIGINT,
    quantity          INT,
    price             DECIMAL(10,2),
    order_total       DECIMAL(12,2),
    order_status      STRING,
    event_time        TIMESTAMP,
    region            STRING,
    country           STRING
) USING iceberg
PARTITIONED BY (days(event_time))
```

### 4.2 Partition Design

**Customers (dimension):**
```
PARTITIONED BY (bucket(64, customer_id))
```
- 50M customers / 64 buckets = ~780K customers per bucket
- ~75 GB / 64 = ~1.2 GB per partition → sweet spot
- **Why bucket on customer_id**: MERGE INTO targets specific customers — bucket partition prunes to 1/64 of the table, making MERGE fast
- **Why NOT partition by is_current or date**: `is_current` has only 2 values (too few, skewed). Date would create many tiny partitions for a slowly-changing dimension.

**Sales (fact):**
```
PARTITIONED BY (days(event_time))
```
- 1.5 GB/day → perfect partition size
- Most queries filter on time range + region → add sort order for region

### 4.3 Sort Order Design

**Customers:**
```sql
ALTER TABLE catalog.customer360.customers WRITE ORDERED BY customer_id, eff_start_date DESC
```
Effective sort order: `[bucket(64, customer_id) ASC, customer_id ASC, eff_start_date DESC]`

**Why:**
- MERGE INTO matches on `customer_id` → sort by customer_id gives tight min/max ranges → file pruning skips 90%+ of files within a bucket
- `eff_start_date DESC` → current version is first in each customer's sorted block → `WHERE is_current = true` reads fewer rows

**Sales:**
```sql
ALTER TABLE catalog.customer360.sales WRITE ORDERED BY region, customer_id
```
Effective sort order: `[days(event_time) ASC, region ASC, customer_id ASC]`

**Why:**
- `region` for regional sales dashboards → file pruning by region
- `customer_id` for customer order history lookups

### 4.4 Write Distribution Mode

**Customers (MERGE INTO heavy):**
```
write.distribution-mode = range   (from WRITE ORDERED BY)
```
- MERGE INTO rewrites files containing matched rows (COW) or writes delete files (MOR)
- MOR is configured: `write.delete.mode = merge-on-read`
- For MOR, distribution mode affects the file layout of newly written data files (inserts from SCD2 new rows)
- RANGE ensures new customer rows are globally sorted → optimal file organization

**Sales (append-heavy):**
```
write.distribution-mode = range   (from WRITE ORDERED BY)
```
- Sales are append-only (no updates) → RANGE sort at write time produces well-organized files
- Each daily partition is small (1.5 GB) → sort overhead is minimal

### 4.5 Read/Write Performance Analysis

**Write: SCD2 MERGE operation (every 15 min):**
```
CDC batch: 500K changes/day ÷ 96 batches/day = ~5,200 changes per batch

MOR path:
  1. Read 5,200 CDC events from Kafka
  2. Dedup by customer_id (keep latest per key)
  3. For each changed customer:
     a. Write position delete file marking old is_current=true row
     b. Update old row: set eff_end_date, is_current=false
     c. Insert new row: new version with is_current=true
  4. Total I/O:
     - Delete files: ~5,200 position deletes ≈ 100 KB
     - Updated rows (COW for the eff_end_date change): ~5,200 × 500 B = 2.6 MB
     - New rows inserted: ~5,200 × 500 B = 2.6 MB
     → Total write per batch: ~5 MB (extremely efficient with MOR)

COW comparison:
  - Must rewrite all files containing the 5,200 changed customers
  - Files affected: ~5,200 / 780K per bucket × 64 buckets = ~0.4 files per bucket on average
  - But each file is 256 MB → rewriting 26+ files = ~6.5 GB per batch
  → COW is 1,300x more expensive per batch!
```

**Read: Current customer profile:**
```
Query: SELECT * FROM customers WHERE customer_id = 12345 AND is_current = true

With bucket partition: prunes to 1/64 of data
With sort order on customer_id: min/max stats prune to ~1 file
With MOR: must merge position deletes → slight overhead

Result: <100ms for point lookups
```

**Read: Point-in-time sales by region (temporal join):**
```sql
SELECT c.country, SUM(s.order_total) as revenue
FROM sales s
JOIN customers c ON s.customer_dim_key = c.customer_dim_key
WHERE s.event_time >= '2026-01-01' AND s.event_time < '2026-04-01'
GROUP BY c.country

Customers table: full scan of is_current and historical rows (75 GB)
  → No time-based pruning (SCD2 rows span arbitrary time ranges)
  → Bucket partition doesn't help for full aggregation
  → This is the trade-off: SCD2 enables history but makes full scans expensive

Sales table: partition pruning on days(event_time) → ~90 days × 1.5 GB = 135 GB scanned
  → Well-sized files with sort order → efficient scan
```

### 4.6 Maintenance Jobs

```
┌────────────────────────────────────────────────────────────────────────┐
│  MAINTENANCE SCHEDULE                                                   │
│                                                                         │
│  EVERY 4 HOURS: Compact MOR delete files (customers table)             │
│    CALL catalog.system.rewrite_data_files(                             │
│      table => 'customer360.customers',                                 │
│      strategy => 'sort',                                               │
│      sort_order => 'customer_id ASC, eff_start_date DESC',             │
│      options => map('delete-file-threshold', '5')                      │
│    )                                                                   │
│    Why: MOR accumulates delete files → must compact to prevent         │
│    read degradation. 96 MERGE batches/day × small delete files.        │
│    After compaction: deletes merged into data files → clean reads.     │
│                                                                         │
│  DAILY: Expire snapshots (customers)                                   │
│    CALL catalog.system.expire_snapshots(                               │
│      table => 'customer360.customers',                                 │
│      older_than => TIMESTAMP 'now - 7 days',                           │
│      retain_last => 50                                                 │
│    )                                                                   │
│    96 commits/day × 7 days = 672 snapshots retained                   │
│                                                                         │
│  DAILY: Expire snapshots (sales)                                       │
│    CALL catalog.system.expire_snapshots(                               │
│      table => 'customer360.sales',                                     │
│      older_than => TIMESTAMP 'now - 30 days',                          │
│      retain_last => 200                                                │
│    )                                                                   │
│    Longer retention for sales: supports time-travel for audit          │
│                                                                         │
│  WEEKLY: Remove orphan files + rewrite manifests                       │
│  MONTHLY: Full sort compaction on customers table (all buckets)        │
└────────────────────────────────────────────────────────────────────────┘
```

### 4.7 Schema Evolution & Partition Evolution Impact

**Schema Evolution — Customer Dimension:**
- **Adding `loyalty_tier`**: `ALTER TABLE customers ADD COLUMN loyalty_tier STRING`
  - Old rows: return `null` for loyalty_tier → queries must use `COALESCE(loyalty_tier, 'unknown')`
  - New CDC events include loyalty_tier → new SCD2 rows have the value
  - Historical queries: old snapshots don't have the column → time-travel returns data without it
  - **No rewrite needed** — this is Iceberg's strength for SCD2 where dimensions grow over time

- **Renaming `address` to `mailing_address`**: safe — Iceberg tracks by field ID
  - CDC pipeline must be updated to map source field to new name
  - Old data files still readable (field ID unchanged)

- **Widening `customer_id` from INT to BIGINT**: safe type promotion
  - Old files with INT values are automatically widened on read

**Partition Evolution — Customer Dimension:**
- Initial: `PARTITIONED BY (bucket(64, customer_id))`
- After growth to 200M customers: partitions become ~4.7 GB each
  - Option 1: increase buckets: `ALTER TABLE customers ADD PARTITION FIELD bucket(256, customer_id)`
  - New files: 256 buckets, old files: 64 buckets — Iceberg handles mixed specs
  - **MERGE INTO impact**: must scan both old (64-bucket) and new (256-bucket) files for matching customer_id → slightly slower until old files are compacted into new partition spec

**Partition Evolution — Sales Fact:**
- Initial: `PARTITIONED BY (days(event_time))`
- If volume grows to 15 GB/day: `ALTER TABLE sales ADD PARTITION FIELD region`
  - New partition: `[days(event_time), region]` → ~3.75 GB per partition (4 regions)
  - Old partitions: still just `days(event_time)` → transparently queried

---

## 5. Further Improvements

**Additional requirement questions:**
- How to handle late-arriving CDC events? (out-of-order changes)
- Is there a need for real-time customer profile serving? (e.g., API cache)
- What about GDPR "right to be forgotten"? (must physically delete customer data)
- How many downstream consumers read the customer 360 data?
- Is there a need for cross-table consistency? (customer + orders atomically consistent)

**Advanced topics:**
- **GDPR deletion**: Use Iceberg's `DELETE FROM customers WHERE customer_id = X` followed by compaction to physically remove data from files. Time-travel to pre-deletion snapshots must be prevented → expire those snapshots.
- **Nessie branches for staging**: Write CDC changes to a branch first, validate, then merge to main. Prevents bad CDC data from corrupting production.
- **Incremental reads**: Downstream consumers can use Iceberg's `incremental-scan` to read only new snapshots since their last checkpoint — enables efficient CDC-to-CDC chains.
- **Z-Order for ad-hoc analytics**: `ZORDER BY (country, loyalty_tier, city)` during monthly compaction for ad-hoc customer segmentation queries.

---

## Appendix: Spark + Iceberg Code Examples

### A1. Create SCD2 customer dimension table

```python
spark.sql("""
CREATE TABLE catalog.customer360.customers (
    customer_dim_key  BIGINT,
    customer_id       BIGINT,
    first_name        STRING,
    last_name         STRING,
    email             STRING,
    phone             STRING,
    address           STRING,
    city              STRING,
    country           STRING,
    loyalty_tier      STRING,
    eff_start_date    DATE,
    eff_end_date      DATE,
    is_current        BOOLEAN,
    source_updated_at TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket(64, customer_id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.parquet.compression-codec' = 'zstd'
)
""")

spark.sql("""
ALTER TABLE catalog.customer360.customers
WRITE ORDERED BY customer_id, eff_start_date DESC
""")
```

### A2. SCD2 merge logic (from PDF's Nova Trends pattern)

```python
from pyspark.sql.functions import lit, current_date, col, when

# Step 1: Read CDC batch from Kafka
cdc_batch = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "customer_changes") \
    .load()

# Step 2: Parse and dedup (keep latest change per customer_id)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("customer_id").orderBy(col("source_updated_at").desc())
cdc_deduped = cdc_batch \
    .select("customer_id", "first_name", "last_name", "email", "phone",
            "address", "city", "country", "loyalty_tier", "source_updated_at") \
    .withColumn("rn", row_number().over(w)) \
    .filter("rn = 1").drop("rn")

# Step 3: Prepare new SCD2 rows with surrogate key
from pyspark.sql.functions import monotonically_increasing_id

new_rows = cdc_deduped \
    .withColumn("customer_dim_key", monotonically_increasing_id()) \
    .withColumn("eff_start_date", current_date()) \
    .withColumn("eff_end_date", lit("9999-12-31").cast("date")) \
    .withColumn("is_current", lit(True))

# Step 4: Find existing current rows to expire
existing = spark.table("catalog.customer360.customers")
join_cond = [existing.customer_id == new_rows.customer_id, existing.is_current == True]

to_expire = existing.join(new_rows, join_cond) \
    .select(
        existing.customer_dim_key, existing.customer_id,
        existing.first_name, existing.last_name, existing.email,
        existing.phone, existing.address, existing.city, existing.country,
        existing.loyalty_tier, existing.eff_start_date,
        new_rows.eff_start_date.alias("eff_end_date"),
        existing.source_updated_at
    ).withColumn("is_current", lit(False))

# Step 5: Union expired + new rows, then MERGE
merged = new_rows.unionByName(to_expire)
merged.createOrReplaceTempView("staged_changes")

spark.sql("""
MERGE INTO catalog.customer360.customers AS target
USING staged_changes AS source
ON target.customer_dim_key = source.customer_dim_key
WHEN MATCHED THEN UPDATE SET
    target.eff_end_date = source.eff_end_date,
    target.is_current = source.is_current
WHEN NOT MATCHED THEN INSERT *
""")
```

### A3. Temporal join: link sales to correct customer version

```python
# From the PDF: join sales to customer dimension using temporal conditions
sales_df = spark.table("catalog.customer360.sales")
customers_df = spark.table("catalog.customer360.customers")

join_cond = [
    sales_df.customer_id == customers_df.customer_id,
    sales_df.event_time >= customers_df.eff_start_date,
    sales_df.event_time < customers_df.eff_end_date
]

result = sales_df.join(customers_df, join_cond, "leftouter") \
    .select(
        sales_df["*"],
        when(customers_df.customer_dim_key.isNull(), lit(-1))
            .otherwise(customers_df.customer_dim_key)
            .alias("customer_dim_key_resolved")
    )
```

### A4. Compaction with MOR delete file cleanup

```python
# Compact customers table: merge delete files back into data files
spark.sql("""
CALL catalog.system.rewrite_data_files(
    table => 'customer360.customers',
    strategy => 'sort',
    sort_order => 'customer_id ASC, eff_start_date DESC',
    options => map(
        'target-file-size-bytes', '268435456',
        'delete-file-threshold', '5',
        'partial-progress.enabled', 'true'
    )
)
""")
```

### A5. Point-in-time query

```python
# What was the customer profile on a specific date?
spark.sql("""
SELECT * FROM catalog.customer360.customers
WHERE customer_id = 12345
  AND eff_start_date <= DATE '2025-06-15'
  AND eff_end_date > DATE '2025-06-15'
""")

# Sales by country at a point in time using time travel
spark.sql("""
SELECT c.country, SUM(s.order_total) as revenue, COUNT(*) as orders
FROM catalog.customer360.sales s
INNER JOIN catalog.customer360.customers c
  ON s.customer_dim_key = c.customer_dim_key
WHERE s.event_time >= '2025-01-01' AND s.event_time < '2025-04-01'
GROUP BY c.country
ORDER BY revenue DESC
""")
```
