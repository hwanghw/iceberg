# Design Problem: ML Feature Store with Apache Iceberg

Design an ML Feature Store for a telecom company (TelCan) that serves as the single source of truth for all ML training data. The system must enforce schema consistency, enable reproducible model training, and support safe experimentation — all without disrupting production ML pipelines. Based on the TelCan use cases from "Apache Iceberg: The Definitive Guide."

---

## 1. Requirements Gathering (Key Questions)

| # | Question | Expected Answer | Design Impact |
|---|----------|----------------|---------------|
| 1 | How many **ML models** consume features, and what's the **training cadence**? | 15 models, retrained weekly on latest data, ad-hoc experimentation daily | Feature tables must support concurrent reads; branches for experiments |
| 2 | What's the **feature data volume**? | 100M customer records, 200+ features, ~50 GB per feature table version | Moderate scale — fits in single Iceberg table, needs good partitioning for fast reads |
| 3 | How often is **new training data ingested**? | Daily batch ingestion from 5 source systems (CRM, billing, network, support tickets, web analytics) | Daily MERGE/APPEND to feature tables; schema enforcement critical |
| 4 | Do **feature schemas change** frequently? | Yes — new features added quarterly (e.g., 5G_Usage_minutes, VoLTE_calls_made), old features rarely removed | Schema evolution without rewriting data; backward compatibility for running models |
| 5 | Is **ML reproducibility** required? | Yes — must reproduce any model's training data exactly, even months later | Time travel, snapshot tags, longer snapshot retention |
| 6 | Do data scientists need **isolated experimentation**? | Yes — need to test new features/data without affecting production feature tables | Iceberg branches for isolated writes; compare experiments on branches vs main |
| 7 | What's the **data quality** requirement? | Strict: schema violations must be caught before write, stakeholders alerted | Schema enforcement on write + pre-write validation logic |

---

## 2. Capacity Estimation

```
Feature Store Tables:
  customer_features:
    records            = 100,000,000
    features           = 200 columns
    avg_record_size    = 500 bytes (Parquet compressed with 200 features)
    table_size         = 100M × 500 B = ~50 GB

  daily_ingestion:
    new_records/day    = 100K (new customers)
    updated_records/day = 2M (feature value changes from billing, network usage)
    daily_delta        = 2.1M × 500 B = ~1 GB

  training_read_per_model:
    data_scanned       = 50 GB (full table for most models)
    with sort+pruning  = 5-15 GB (if model trains on specific customer segments)
    training_duration  = 30-60 min per model

Partition sizing:
  PARTITIONED BY (bucket(32, customer_id))
  partition_size      = 50 GB / 32 = ~1.6 GB per bucket ← sweet spot
  files_per_partition = 1.6 GB / 256 MB = ~6 files per bucket

Branch storage overhead:
  branch = pointer to snapshot (no data copy initially)
  writes to branch: only new/changed files stored
  cost: near-zero until data diverges significantly

Tag storage overhead:
  tag = pointer to snapshot_id (zero additional storage)
  100 tags retained = 100 snapshot pointers

Snapshot retention for reproducibility:
  models retrained weekly × 52 weeks = 52 tagged snapshots/year
  each snapshot: metadata pointer only (data files shared)
  storage overhead: ~52 × 2 KB = negligible
  BUT: must NOT expire tagged snapshots → configure min_snapshots_to_keep
```

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    ML FEATURE STORE ARCHITECTURE                          │
│                                                                          │
│  ┌────────────────────────────────────────┐                             │
│  │          SOURCE SYSTEMS                 │                             │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐  │                             │
│  │  │ CRM  │ │Billing│ │Network│ │Support│  │                            │
│  │  └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘  │                             │
│  └─────┼────────┼────────┼────────┼───────┘                             │
│        └────────┴────────┴────────┘                                      │
│                     │                                                     │
│                     ▼                                                     │
│        ┌──────────────────────┐                                          │
│        │  Ingestion Pipeline   │                                         │
│        │  (Spark batch, daily) │                                         │
│        │                       │                                         │
│        │  1. Extract from srcs │                                         │
│        │  2. Schema validation │ ← Catches type mismatches before write  │
│        │  3. Feature engineering│                                        │
│        │  4. MERGE INTO Iceberg│                                         │
│        └──────────┬────────────┘                                         │
│                   │                                                       │
│                   ▼                                                       │
│  ┌───────────────────────────────────────────────────────────┐          │
│  │              ICEBERG FEATURE TABLES                        │          │
│  │                                                            │          │
│  │  ┌─────────────────────┐   ┌─────────────────────┐       │          │
│  │  │ customer_features   │   │ network_features     │       │          │
│  │  │ (main branch)       │   │ (main branch)        │       │          │
│  │  │                     │   │                      │       │          │
│  │  │ Tags:               │   │ Branches:            │       │          │
│  │  │  - June_23          │   │  - experiment_5g     │       │          │
│  │  │  - model_v2_train   │   │  - test_volte_feats  │       │          │
│  │  │  - quarterly_review │   │                      │       │          │
│  │  └─────────────────────┘   └─────────────────────┘       │          │
│  │                                                            │          │
│  │  Iceberg Capabilities Used:                                │          │
│  │  • Schema enforcement (reject bad writes)                  │          │
│  │  • Schema evolution (ADD COLUMN without rewrite)           │          │
│  │  • Time travel (read any historical snapshot)              │          │
│  │  • Tags (label snapshots for ML versioning)                │          │
│  │  • Branches (isolated experimentation)                     │          │
│  └───────────────────────────────────────────────────────────┘          │
│                   │                                                       │
│        ┌──────────┴──────────┐                                           │
│        ▼                     ▼                                           │
│  ┌──────────────┐   ┌───────────────────┐                               │
│  │  Production   │   │  Experimentation   │                              │
│  │  ML Training  │   │  ML Training       │                              │
│  │  (Spark MLlib)│   │  (reads branches)  │                              │
│  │              │   │                    │                               │
│  │  Reads main   │   │  Reads branch_     │                              │
│  │  or tagged    │   │  Churn_PremiumExp  │                              │
│  │  snapshots    │   │                    │                              │
│  └──────┬───────┘   └────────┬───────────┘                              │
│         │                    │                                           │
│         ▼                    ▼                                           │
│  ┌──────────────────────────────────────┐                               │
│  │       Model Registry & Serving        │                              │
│  │  (MLflow / SageMaker / Kubeflow)      │                              │
│  └──────────────────────────────────────┘                               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Deep Dive

### 4.1 Schema Design

```sql
CREATE TABLE catalog.features.customer_features (
    customer_id         BIGINT,
    -- Demographics
    state               STRING,
    area_code           INT,
    account_length      INT,
    -- Usage metrics
    total_day_minutes    FLOAT,
    total_day_calls      INT,
    total_day_charge     FLOAT,
    total_eve_minutes    FLOAT,
    total_eve_calls      INT,
    total_eve_charge     FLOAT,
    total_night_minutes  FLOAT,
    total_night_calls    INT,
    total_night_charge   FLOAT,
    total_intl_minutes   FLOAT,
    total_intl_calls     INT,
    total_intl_charge    FLOAT,
    -- Service interactions
    customer_service_calls INT,
    international_plan   BOOLEAN,
    voice_mail_plan      BOOLEAN,
    number_vmail_messages INT,
    -- Target variable
    churn               BOOLEAN,
    -- Metadata
    feature_version     INT,           -- tracks which feature engineering version produced this row
    ingested_at         TIMESTAMP      -- when this row was last updated
) USING iceberg
PARTITIONED BY (bucket(32, customer_id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
```

**Why this schema:**
- Flat, wide table — ML-friendly (each row = one training sample)
- `feature_version` tracks which pipeline version produced the features — critical for debugging model drift
- `ingested_at` enables incremental reads and freshness monitoring
- No SCD2 columns here (unlike customer360) — this is a "latest state" feature table. Historical versions accessed via time travel/tags instead.

### 4.2 Partition Design

```
PARTITIONED BY (bucket(32, customer_id))
```

**Analysis:**
- 100M records / 32 buckets = ~3.1M records per bucket
- ~50 GB / 32 = ~1.6 GB per partition → sweet spot
- **Why bucket**: Most ML training reads the full table → bucket doesn't help for full scans BUT helps enormously for MERGE INTO (daily ingestion targets specific customers)
- **Alternative — no partition**: For pure ML training (full scan), unpartitioned + sort order may be simpler. But daily MERGE would scan entire table without partition pruning → too slow at 50 GB.

### 4.3 Sort Order Design

```sql
ALTER TABLE catalog.features.customer_features WRITE ORDERED BY customer_id
```

**Effective sort order:** `[bucket(32, customer_id) ASC, customer_id ASC]`

**Why:**
- MERGE INTO on `customer_id` → sort gives tight min/max → fast lookups for 2M updates/day
- ML training often doesn't benefit from sort (reads all rows anyway)
- But segment-specific training (e.g., train only on state='CA') benefits from adding `state` to sort order

### 4.4 Write Distribution Mode

```
write.distribution-mode = range   (from WRITE ORDERED BY)
```

**Analysis:**
- Daily MERGE writes ~2.1M rows → RANGE sorts them globally by `[bucket, customer_id]`
- New data files have tight, non-overlapping customer_id ranges per bucket
- Benefit: next day's MERGE can prune files efficiently (only touch files containing changed customer_ids)
- **Trade-off**: RANGE shuffle is more expensive than HASH for writes
  - At 1 GB/day delta, the overhead is negligible (~seconds of extra shuffle time)
  - Payoff: much faster MERGE on subsequent days

### 4.5 Read/Write Performance Analysis

**Write: Daily feature ingestion:**
```
Spark batch job reads from 5 source systems
  → Feature engineering transforms
  → Schema validation (compare against Iceberg table schema)
  → MERGE INTO customer_features
    - 100K new inserts + 2M updates = 2.1M rows
    - With bucket(32) partition: prunes to affected buckets
    - With sort order: updates target specific files within buckets
    - Write I/O: ~1 GB new data + rewrite of ~3 GB affected files = ~4 GB total
    - Duration: ~10 minutes
```

**Write: Schema evolution (adding new features):**
```python
# As shown in the PDF (TelCan adding 5G features):
spark.sql("ALTER TABLE catalog.features.customer_features ADD COLUMNS (5G_Usage_minutes FLOAT, VoLTE_calls_made INT)")
# Zero I/O — metadata-only operation (< 1 second)
# Old files: return null for new columns on read
# New ingestion: populates the new columns
```

**Read: Production ML training:**
```
Full table scan: 50 GB
  → 32 partitions × ~6 files each = ~192 files
  → Spark reads all files in parallel
  → With Parquet column pruning: if model uses 50 of 200 features,
    only ~25% of column data is read = ~12.5 GB actual I/O
  → Training time: ~30 minutes including feature preprocessing
```

**Read: Reproducible training with tag:**
```python
# Read exactly the data used for model_v2 training
df = spark.read.option("tag", "model_v2_train").format("iceberg").load("catalog.features.customer_features")
# Same data as when tag was created — guaranteed bit-for-bit identical
```

**Read: Experimental branch:**
```python
# Read from experiment branch (includes new experimental features)
df = spark.read.option("branch", "Churn_PremiumExp").format("iceberg").load("catalog.features.customer_features")
# Returns 2,175 rows (main has 1,240) — extra experimental data
# Main table completely unaffected
```

### 4.6 Maintenance Jobs

```
┌────────────────────────────────────────────────────────────────────────┐
│  MAINTENANCE SCHEDULE                                                   │
│                                                                         │
│  DAILY (after ingestion): Compaction                                   │
│    CALL catalog.system.rewrite_data_files(                             │
│      table => 'features.customer_features',                            │
│      strategy => 'sort',                                               │
│      sort_order => 'customer_id ASC'                                   │
│    )                                                                   │
│    Why: MERGE INTO creates new files for changed rows →               │
│    accumulates small files over time. Sort compaction restores         │
│    optimal file layout.                                                │
│                                                                         │
│  WEEKLY: Expire snapshots (CAREFUL with tags!)                         │
│    CALL catalog.system.expire_snapshots(                               │
│      table => 'features.customer_features',                            │
│      older_than => TIMESTAMP 'now - 30 days',                          │
│      retain_last => 100                                                │
│    )                                                                   │
│    ⚠ Tags protect their snapshots from expiration!                    │
│    Snapshots referenced by tags are NOT expired.                       │
│    This is essential for ML reproducibility.                           │
│                                                                         │
│  WEEKLY: Remove orphan files                                           │
│    CALL catalog.system.remove_orphan_files(                            │
│      table => 'features.customer_features',                            │
│      older_than => TIMESTAMP 'now - 7 days'                            │
│    )                                                                   │
│                                                                         │
│  MONTHLY: Rewrite manifests                                            │
│    CALL catalog.system.rewrite_manifests(                              │
│      table => 'features.customer_features'                             │
│    )                                                                   │
│                                                                         │
│  QUARTERLY: Clean up old branches                                      │
│    -- Drop completed experiment branches                               │
│    ALTER TABLE catalog.features.customer_features                      │
│      DROP BRANCH IF EXISTS experiment_5g_complete                      │
│    -- Keep tags indefinitely for reproducibility                       │
└────────────────────────────────────────────────────────────────────────┘
```

### 4.7 Schema Evolution & Partition Evolution Impact

**Schema Evolution — The Core ML Challenge:**

From the PDF: TelCan introduces 5G and VoLTE features:

```python
spark.sql("""
ALTER TABLE catalog.features.customer_features
ADD COLUMNS (5G_Usage_minutes FLOAT, VoLTE_calls_made INT)
""")
```

**Impact on existing ML pipelines:**
- Running production model (trained on old schema): reads old files → new columns return `null` → model ignores them (if using column selection). **No disruption.**
- New model version: includes new features → trained on post-evolution data with values populated
- **Reproducibility preserved**: time travel to pre-evolution snapshots returns data WITHOUT new columns. Training pipeline for old model version reproduces exactly.

**Schema evolution + branches pattern:**
1. Create branch: `ALTER TABLE customer_features CREATE BRANCH test_5g_features`
2. Add columns to branch only (write new data to branch)
3. Train experimental model on branch data
4. If experiment succeeds: apply schema change to main, merge data
5. If experiment fails: drop branch → zero impact on main

**Partition Evolution:**
- Unlikely to change for feature store (stable customer_id bucketing)
- If needed (e.g., growing from 100M to 1B customers): add more buckets
  - `ALTER TABLE customer_features ADD PARTITION FIELD bucket(128, customer_id)`
  - Old files: 32 buckets, new files: 128 buckets
  - MERGE INTO must scan both specs → slightly slower until compacted
  - **ML training unaffected**: full scan reads all files regardless of partition spec

---

## 5. Further Improvements

**Additional requirement questions:**
- Is there a need for real-time feature serving (online feature store)?
- How to handle feature versioning across multiple models?
- What monitoring is needed for feature drift detection?
- Are there access control requirements (who can write to feature tables)?
- How to handle backfilling features when a new source system is added?

**Advanced topics:**
- **Nessie catalog for multi-table branching**: Branch all feature tables atomically across the catalog (not just table-level branches). Enables consistent experiments across multiple tables.
- **Feature lineage**: Track which source columns contributed to each feature. Iceberg's schema field IDs + metadata tables can help.
- **Online/offline feature store split**: Offline store = Iceberg tables (batch training). Online store = Redis/DynamoDB (real-time serving). Sync materialized features from Iceberg to online store.
- **Automated schema validation**: Pre-write validation as shown in the PDF (TelCan's `validate_and_ingest` function). Catches type mismatches before they reach the Iceberg table.

---

## Appendix: Spark + Iceberg Code Examples

### A1. Schema enforcement (from PDF)

```python
# Iceberg rejects incompatible schemas on write automatically:
# Error: Cannot write incompatible data to table 'Iceberg catalog.features.customer_features':
# - Cannot safely cast 'Account_length': string to int

# Explicit pre-write validation:
def validate_and_ingest(new_data_path, iceberg_table_name):
    table_schema = spark.table(iceberg_table_name).schema
    new_df = spark.read.csv(new_data_path, header=True, inferSchema=True)

    # Compare schemas
    for field in table_schema:
        if field.name in new_df.columns:
            if new_df.schema[field.name].dataType != field.dataType:
                raise ValueError(
                    f"Schema mismatch: {field.name} expected {field.dataType}, "
                    f"got {new_df.schema[field.name].dataType}"
                )

    # Schema valid — append
    new_df.writeTo(iceberg_table_name).append()
    print(f"Successfully ingested {new_df.count()} rows")
```

### A2. Schema evolution (from PDF)

```python
# Add new features without disrupting existing pipelines
spark.sql("""
ALTER TABLE catalog.features.customer_features
ADD COLUMNS (5G_Usage_minutes FLOAT, VoLTE_calls_made INT)
""")

# Ingest new data with the evolved schema
df_evolved = spark.read.csv("new_features_with_5g.csv", header=True, inferSchema=True)
df_evolved.writeTo("catalog.features.customer_features").append()
```

### A3. Time travel for ML reproducibility (from PDF)

```python
# View snapshot history
spark.sql("SELECT * FROM catalog.features.customer_features.history").show()

# Read data as of a specific timestamp
df_old = spark.sql("""
SELECT * FROM catalog.features.customer_features
TIMESTAMP AS OF '2025-05-03 19:24:24.418'
""")

# Read data by snapshot ID
snapshot_id = 5889239598709613914
df_snapshot = spark.read \
    .option("snapshot-id", snapshot_id) \
    .format("iceberg") \
    .load("catalog.features.customer_features")

# Train model on specific historical data
pdf = df_snapshot.toPandas()
# ... sklearn / xgboost / pytorch model training ...
```

### A4. Tags for dataset versioning (from PDF)

```python
# Create a tag after ingestion for model training reference
spark.sql("ALTER TABLE catalog.features.customer_features CREATE TAG model_v2_train")

# View all tags
spark.sql("SELECT * FROM catalog.features.customer_features.refs").show()
# name          type    snapshot_id
# main          BRANCH  2496871934354606665
# model_v2_train TAG    2496871934354606665

# Read tagged dataset for reproducible training
df = spark.read \
    .option("tag", "model_v2_train") \
    .format("iceberg") \
    .load("catalog.features.customer_features")
```

### A5. Branches for experimentation (from PDF)

```python
# Create isolated experiment branch
spark.sql("ALTER TABLE catalog.features.customer_features CREATE BRANCH Churn_PremiumExp")

# Write experimental data to branch (main unaffected)
df_exp = spark.read.csv("experimental_premium_features.csv", header=True, inferSchema=True)
df_exp.write.format("iceberg").mode("append") \
    .save("catalog.features.customer_features.branch_Churn_PremiumExp")

# Verify main is unaffected
main_count = spark.table("catalog.features.customer_features").count()
branch_count = spark.read.format("iceberg") \
    .load("catalog.features.customer_features.branch_Churn_PremiumExp").count()
print(f"Main: {main_count} rows, Branch: {branch_count} rows")
# Main: 1240 rows, Branch: 2175 rows

# Train model on branch data
df_branch = spark.read \
    .option("branch", "Churn_PremiumExp") \
    .format("iceberg") \
    .load("catalog.features.customer_features")

# If experiment succeeds: promote branch to main (fast-forward merge)
# If experiment fails: drop branch
spark.sql("ALTER TABLE catalog.features.customer_features DROP BRANCH Churn_PremiumExp")
```

### A6. Maintenance

```python
# Compact after daily ingestion
spark.sql("""
CALL catalog.system.rewrite_data_files(
    table => 'features.customer_features',
    strategy => 'sort',
    sort_order => 'customer_id ASC',
    options => map('target-file-size-bytes', '268435456')
)
""")

# Expire snapshots (tags are protected)
spark.sql("""
CALL catalog.system.expire_snapshots(
    table => 'features.customer_features',
    older_than => TIMESTAMP '2026-02-23 00:00:00',
    retain_last => 100
)
""")

# Rewrite manifests
spark.sql("CALL catalog.system.rewrite_manifests(table => 'features.customer_features')")
```
