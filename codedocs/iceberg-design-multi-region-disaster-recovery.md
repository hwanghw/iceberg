# Design Problem: Multi-Region Data Lakehouse with Disaster Recovery

Design a multi-region data lakehouse using Apache Iceberg that provides disaster recovery (DR) capabilities with defined RPO/RTO targets. The system serves a global financial services company with regulatory requirements for data residency and business continuity.

---

## 1. Requirements Gathering (Key Questions)

| # | Question | Expected Answer | Design Impact |
|---|----------|----------------|---------------|
| 1 | What's the **RPO** (Recovery Point Objective)? | < 15 minutes data loss acceptable | Async replication with 10-min lag (not sync — too expensive) |
| 2 | What's the **RTO** (Recovery Time Objective)? | < 1 hour to resume queries | Pre-warmed standby catalog + compute; automated failover |
| 3 | What's the **total data volume** and daily ingest? | 500 TB total, 2 TB/day ingest | S3 cross-region replication bandwidth, replication lag budget |
| 4 | Is it **active-active** or **active-standby**? | Active-standby (writes only in primary) | Simplifies consistency — no cross-region write conflicts |
| 5 | What **regulatory constraints** exist? | Data must not leave certain regions; EU data in EU region | Separate tables per region or row-level filtering; not full replication for regulated data |
| 6 | What **engines** read/write the data? | Spark writes (primary), Trino reads (both regions) | Need catalog accessible from both regions; read replicas for standby |
| 7 | How many **tables** and what's the **metadata size**? | 5,000 tables, metadata ~50 GB total | Catalog replication must handle metadata sync efficiently |

---

## 2. Capacity Estimation

```
Data Volume:
  total_data           = 500 TB
  daily_ingest         = 2 TB/day
  cross_region_repl    = 2 TB/day (all new data replicated)
  replication_bandwidth = 2 TB / 86,400 sec = ~24 MB/s sustained

  S3 cross-region replication cost:
    data_transfer       = 2 TB/day × $0.02/GB = ~$40/day = ~$1,200/month
    S3 storage (standby) = 500 TB × $0.023/GB = ~$11,500/month
    Total DR storage     = ~$12,700/month

Metadata:
  tables               = 5,000
  avg_snapshots/table   = 100
  metadata_per_table    = ~10 MB (metadata.json + manifest lists)
  total_metadata        = 5,000 × 10 MB = ~50 GB
  metadata_replication  = 50 GB × daily_churn_rate (~5%) = ~2.5 GB/day

RPO Analysis:
  S3 replication lag    = typically 5-10 minutes (async)
  Catalog sync lag      = depends on sync mechanism (5-15 min)
  Effective RPO         = max(S3_lag, catalog_lag) = ~15 minutes

RTO Analysis:
  catalog_failover      = 2-5 min (DNS switch + cache warm)
  compute_startup       = 5-10 min (if pre-provisioned: 0 min)
  state_validation      = 10-15 min (verify latest metadata consistent)
  total_RTO             = ~20-30 min (with pre-provisioned standby)
```

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              MULTI-REGION DISASTER RECOVERY ARCHITECTURE                      │
│                                                                              │
│  PRIMARY REGION (us-east-1)              STANDBY REGION (eu-west-1)         │
│  ┌──────────────────────────┐            ┌──────────────────────────┐       │
│  │  ┌────────┐ ┌─────────┐ │            │  ┌────────┐ ┌─────────┐ │       │
│  │  │ Spark  │ │  Trino  │ │            │  │ Trino  │ │ (Spark  │ │       │
│  │  │ Writer │ │ Reader  │ │            │  │ Reader │ │ standby)│ │       │
│  │  └───┬────┘ └────┬────┘ │            │  └───┬────┘ └────┬────┘ │       │
│  │      │           │      │            │      │           │      │       │
│  │  ┌───▼───────────▼────┐ │            │  ┌───▼───────────▼────┐ │       │
│  │  │ REST Catalog       │ │◄── sync ──▶│  │ REST Catalog       │ │       │
│  │  │ (Primary)          │ │  (10 min)  │  │ (Read Replica)     │ │       │
│  │  └───┬────────────────┘ │            │  └───┬────────────────┘ │       │
│  │      │                  │            │      │                  │       │
│  │  ┌───▼────────────────┐ │            │  ┌───▼────────────────┐ │       │
│  │  │ S3 Bucket          │ │◄── S3  ───▶│  │ S3 Bucket          │ │       │
│  │  │ (Source)            │ │  CRR       │  │ (Replica)          │ │       │
│  │  │ data/ + metadata/  │ │ (async)    │  │ data/ + metadata/  │ │       │
│  │  └────────────────────┘ │            │  └────────────────────┘ │       │
│  └──────────────────────────┘            └──────────────────────────┘       │
│                                                                              │
│  FAILOVER FLOW:                                                             │
│  1. Detect primary failure (health check, 2 min)                            │
│  2. Verify S3 replica consistency (metadata.json matches data files)        │
│  3. Promote standby catalog to primary (DNS switch)                         │
│  4. Start Spark writers in standby region                                   │
│  5. Resume operations (total: ~20-30 min)                                   │
│                                                                              │
│  FAILBACK:                                                                  │
│  1. Restore primary region                                                  │
│  2. Replicate delta from standby back to primary                            │
│  3. Switch catalog back                                                     │
│  4. Resume normal operations                                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3A. Replication Strategy: Two Alternatives

There are two fundamentally different approaches to replicating Iceberg tables across regions. Each has distinct trade-offs.

### Alternative A: AWS S3 Cross-Region Replication (CRR)

**How it works:** S3 CRR automatically copies newly created objects from a source bucket to a destination bucket in a different AWS region. Since Iceberg stores all data files, manifest files, and metadata files as S3 objects, CRR replicates the entire table transparently.

```
┌────────────────────────────────────────────────────────────────────────┐
│  ALTERNATIVE A: S3 CRR                                                  │
│                                                                         │
│  PRIMARY (us-east-1)                    STANDBY (eu-west-1)            │
│  ┌──────────────────────┐               ┌──────────────────────┐      │
│  │ s3://primary-bucket/  │  ── S3 CRR ─▶ │ s3://standby-bucket/  │     │
│  │  data/                │  (automatic,  │  data/                │     │
│  │  metadata/            │   async,      │  metadata/            │     │
│  │  manifests/           │   ~15 min)    │  manifests/           │     │
│  └──────────────────────┘               └──────────────────────┘      │
│                                                                         │
│  Catalog sync handled SEPARATELY (not part of S3 CRR):                 │
│  ┌──────────────────────┐               ┌──────────────────────┐      │
│  │ REST Catalog          │  ── custom ─▶ │ REST Catalog          │     │
│  │ (primary)             │    sync job   │ (standby, read-only)  │     │
│  └──────────────────────┘               └──────────────────────┘      │
│                                                                         │
│  ⚠ S3 CRR replicates files but does NOT update the standby catalog.  │
│  The standby catalog must be synced separately so that query engines   │
│  in the standby region can discover the latest table state.            │
└────────────────────────────────────────────────────────────────────────┘
```

**Prerequisites (from AWS docs):**

| Requirement | Details |
|---|---|
| **Versioning** | Must be enabled on BOTH source and destination buckets. Cannot disable versioning while CRR is active. |
| **IAM permissions** | Source account needs `s3:ReplicateObject`, `s3:ReplicateDelete`, `s3:GetReplicationConfiguration` on destination |
| **Bucket policy** | For cross-account: destination owner must grant source account replication permissions via bucket policy |
| **Regions** | Both regions must be enabled in the AWS account |

**What IS replicated:**

| Replicated | Details |
|---|---|
| New objects | All objects created AFTER CRR is configured |
| Object metadata | Timestamps, content-type, user metadata preserved identically |
| Object tags | Replicated if present at PutObject time |
| Encrypted objects | SSE-S3, SSE-KMS, DSSE-KMS supported (SSE-C requires special handling) |
| ACL updates | Unless cross-account ownership override is configured |
| Object Lock retention | Replica inherits source retention settings |

**What is NOT replicated:**

| Not Replicated | Impact on Iceberg DR |
|---|---|
| **Existing objects** (pre-CRR) | Must use S3 Batch Replication for initial sync of 500 TB historical data |
| **Delete markers** (by default) | Protects standby from accidental/malicious deletes in primary. Can be explicitly enabled. **For Iceberg DR: keep disabled** — standby should retain all files even if primary expires them. |
| **Lifecycle actions** | S3 lifecycle transitions (e.g., to Glacier) are NOT replicated. Standby retains original storage class. |
| **Objects in Glacier/Deep Archive** | Archived objects are not replicated |
| **Replicas of replicas** | Cannot chain CRR (A→B→C). Use Batch Replication instead. |
| **Bucket-level configs** | Lifecycle rules, notification configs NOT replicated — set separately on standby |

**S3 Replication Time Control (RTC):**
- **SLA**: 99.99% of new objects replicated within **15 minutes**
- Backed by AWS SLA — eligible for service credits if missed
- Provides CloudWatch metrics: `ReplicationLatency`, `OperationsPendingReplication`, `BytesPendingReplication`
- **Cost**: additional ~$0.015/GB on top of standard CRR data transfer

**Cost breakdown for our 500 TB / 2 TB-per-day scenario:**

```
Monthly costs:
  S3 CRR data transfer    = 2 TB/day × 30 × $0.02/GB    = ~$1,200/month
  S3 RTC premium (if used) = 2 TB/day × 30 × $0.015/GB   = ~$900/month
  Standby storage          = 500 TB × $0.023/GB           = ~$11,500/month
  S3 PUT requests (dest)   = ~80K files/day × 30 × $5/M  = ~$12/month
  ─────────────────────────────────────────────────────────
  Total without RTC        = ~$12,712/month
  Total with RTC           = ~$13,612/month
```

**Key limitation for Iceberg:**
S3 CRR is "dumb" — it replicates all objects in the bucket but has **no awareness of Iceberg table structure**. This means:
- It replicates orphan files (files from failed commits that aren't referenced by any snapshot)
- It replicates files that will be deleted by snapshot expiration (wasted bandwidth)
- It cannot selectively replicate specific tables or partitions (only S3 prefix-based rules)
- **Catalog sync is a separate problem** — CRR copies files but the standby catalog must be updated separately to point to the latest metadata

#### Catalog Sync Mechanism for S3 CRR

S3 CRR handles file replication but **does not update the standby catalog**. Without catalog
sync, query engines in the standby region don't know about new snapshots. You must build
a separate sync mechanism. Here are three approaches, from simplest to most robust:

**Approach 1: Metadata-location-based catalog (no sync needed)**

If the query engine can read the Iceberg `metadata.json` directly from S3 instead of going
through a catalog, no sync is needed — S3 CRR replicates the metadata files and engines
discover them automatically.

```
┌────────────────────────────────────────────────────────────────────────┐
│  APPROACH 1: HADOOP/FILE-BASED CATALOG (simplest)                       │
│                                                                         │
│  Primary writes to:                                                    │
│    s3://primary/warehouse/db/orders/metadata/v12.metadata.json         │
│                                                                         │
│  S3 CRR replicates to:                                                 │
│    s3://standby/warehouse/db/orders/metadata/v12.metadata.json         │
│                                                                         │
│  Standby Spark/Trino uses Hadoop catalog:                              │
│    spark.sql.catalog.standby.type = hadoop                             │
│    spark.sql.catalog.standby.warehouse = s3://standby/warehouse/       │
│                                                                         │
│  How it works:                                                         │
│    Hadoop catalog finds the latest metadata.json by listing the        │
│    metadata/ directory and picking the highest version number.          │
│    S3 CRR replicates all metadata files → engine auto-discovers.      │
│                                                                         │
│  ✓ Zero sync code needed                                              │
│  ✗ Hadoop catalog is deprecated / not recommended for production      │
│  ✗ S3 LIST operations are eventually consistent (rare stale reads)    │
│  ✗ No catalog-level features (branching, tagging, access control)     │
└────────────────────────────────────────────────────────────────────────┘
```

**Approach 2: Periodic catalog sync job (recommended for REST/Glue catalog)**

A scheduled job reads the latest `metadata.json` location from the primary catalog and
updates the standby catalog to point to the same file (at the standby S3 path).

```
┌────────────────────────────────────────────────────────────────────────┐
│  APPROACH 2: PERIODIC CATALOG SYNC JOB                                  │
│                                                                         │
│  Runs every 10-15 minutes (matches S3 RTC SLA):                       │
│                                                                         │
│  For each table in the primary catalog:                                │
│                                                                         │
│  Step 1: GET latest metadata location from primary catalog             │
│    primary_metadata = primary_catalog                                  │
│      .loadTable("db.orders")                                           │
│      .operations().current().metadataFileLocation()                    │
│    // e.g., "s3://primary/warehouse/db/orders/metadata/v12.metadata.json"
│                                                                         │
│  Step 2: TRANSLATE path to standby region                              │
│    standby_metadata = primary_metadata.replace(                        │
│      "s3://primary/", "s3://standby/")                                 │
│    // → "s3://standby/warehouse/db/orders/metadata/v12.metadata.json" │
│                                                                         │
│  Step 3: VERIFY the metadata file exists in standby (CRR may lag)     │
│    if NOT exists(standby_metadata):                                    │
│      log("CRR hasn't replicated v12 yet — skip this cycle")          │
│      continue  // try again next cycle                                │
│                                                                         │
│  Step 4: VERIFY all data files referenced by this metadata exist      │
│    Read standby_metadata → parse manifest list → parse manifests      │
│    For each DataFile: HEAD request to verify existence                 │
│    If any missing: skip — CRR still catching up                       │
│                                                                         │
│  Step 5: UPDATE standby catalog to point to new metadata              │
│    standby_catalog.registerTable("db.orders", standby_metadata)       │
│    // or: update existing table's metadata pointer                    │
│                                                                         │
│  This is the SAME atomic consistency protocol as Alternative B:       │
│    Files exist (CRR copied them) → verified (Step 4) → pointer swap  │
│                                                                         │
│  ✓ Works with REST Catalog, Glue Catalog, any catalog                 │
│  ✓ Atomic — standby never points to incomplete metadata              │
│  ✓ Tolerant of CRR lag — skips cycles until files are ready          │
│  ✗ Requires custom sync job (Airflow DAG or Lambda)                  │
│  ✗ RPO = max(CRR lag, sync interval) — typically 15-20 min          │
└────────────────────────────────────────────────────────────────────────┘
```

**Implementation detail — updating an existing table in the standby catalog:**

`Catalog.registerTable()` throws `AlreadyExistsException` if the table exists. For incremental
sync, you need to update the existing table's metadata pointer. Two options:

```
Option A: Drop and re-register (simple but not atomic)
  standby_catalog.dropTable(identifier, false)  // purge=false, keep files
  standby_catalog.registerTable(identifier, standby_metadata)
  ⚠ Brief window where table doesn't exist — queries fail

Option B: Use catalog's updateTableMetadata API (atomic, if supported)
  REST Catalog: POST /v1/tables/{table}/metadata with new metadata location
  Glue Catalog: UpdateTable API with new metadata_location parameter
  → No gap — pointer swaps atomically

Option C: Use Iceberg Transaction API
  Table standbyTable = standby_catalog.loadTable(identifier)
  // Point to new metadata by creating a "fast-forward" transaction
  // This depends on catalog implementation support
```

**Approach 3: Event-driven catalog sync (lowest RPO)**

Use S3 Event Notifications to trigger catalog sync immediately when `metadata.json`
files are replicated, instead of polling on a schedule.

```
┌────────────────────────────────────────────────────────────────────────┐
│  APPROACH 3: EVENT-DRIVEN CATALOG SYNC                                  │
│                                                                         │
│  S3 standby bucket                                                     │
│    │                                                                   │
│    │ S3 Event Notification (on PutObject)                              │
│    │ Filter: prefix=warehouse/, suffix=metadata.json                  │
│    │                                                                   │
│    ▼                                                                   │
│  SQS Queue / EventBridge                                               │
│    │                                                                   │
│    ▼                                                                   │
│  Lambda / Step Function                                                │
│    │                                                                   │
│    │  1. Parse event: extract table name from S3 key                  │
│    │     s3://standby/warehouse/db/orders/metadata/v12.metadata.json  │
│    │     → table = "db.orders"                                        │
│    │                                                                   │
│    │  2. Wait for all referenced files to be present                  │
│    │     Read metadata.json → list all referenced file paths          │
│    │     Poll until all exist (S3 CRR usually completes data files   │
│    │     before metadata — metadata.json is written LAST by Iceberg) │
│    │                                                                   │
│    │  3. Update standby catalog pointer                               │
│    │     REST API call or Glue UpdateTable                            │
│    │                                                                   │
│    ▼                                                                   │
│  Standby catalog updated within seconds of CRR completing             │
│                                                                         │
│  ✓ Lowest RPO (~seconds after CRR completes)                         │
│  ✓ No polling — event-driven, cost-efficient                         │
│  ✓ Scales to thousands of tables                                     │
│  ✗ Most complex to set up (SQS, Lambda, error handling, DLQ)        │
│  ✗ Must handle out-of-order events (v13 may arrive before v12)      │
│  ✗ Must handle duplicate events (SQS at-least-once)                 │
└────────────────────────────────────────────────────────────────────────┘
```

**Important: Iceberg's metadata write order helps here.**

When Iceberg commits a snapshot, it writes files in this order:
1. Data files (Parquet) — written first
2. Manifest files (Avro) — written after data files
3. Manifest list (Avro) — written after manifests
4. `metadata.json` — written **LAST**

S3 CRR replicates objects in the order they were created. So by the time `metadata.json`
arrives in the standby bucket, all the data files it references have **likely already been
replicated** (they were written earlier). This means the verification step in Approach 2/3
usually passes on the first check.

However, "likely" is not "guaranteed" — S3 CRR does not guarantee ordering across objects.
The verification step is still essential for correctness.

**Comparison of catalog sync approaches:**

| Aspect | Approach 1: Hadoop | Approach 2: Periodic Job | Approach 3: Event-Driven |
|---|---|---|---|
| **Setup complexity** | None | Medium (Airflow DAG) | High (SQS + Lambda + DLQ) |
| **RPO** | = CRR lag | CRR lag + sync interval | ~CRR lag + seconds |
| **Catalog features** | No branching/tagging/ACL | Full catalog features | Full catalog features |
| **Atomicity** | None (LIST-based) | Yes (verify → swap) | Yes (verify → swap) |
| **Scalability** | Poor (S3 LIST per query) | Good (batch all tables) | Best (per-table, parallel) |
| **Recommended for** | Dev/test only | Production (most cases) | Production (low RPO) |

---

### Alternative B: Iceberg API-Based Replication

**How it works:** Use Iceberg's built-in `RewriteTablePath` action to rewrite table metadata with new file paths pointing to the standby region, generate a copy plan of all referenced files, copy only those files, and register the table in the standby catalog.

```
┌────────────────────────────────────────────────────────────────────────┐
│  ALTERNATIVE B: ICEBERG API REPLICATION                                 │
│                                                                         │
│  Step 1: REWRITE METADATA (Spark job in primary region)                │
│  ┌──────────────────────────────────────────────────────────┐          │
│  │  SparkActions.get().rewriteTablePath(sourceTable)         │          │
│  │    .rewriteLocationPrefix(                                │          │
│  │        "s3://primary-bucket/warehouse",                   │          │
│  │        "s3://standby-bucket/warehouse")                   │          │
│  │    .startVersion("v3.metadata.json")   // incremental    │          │
│  │    .endVersion("v5.metadata.json")                        │          │
│  │    .stagingLocation("s3://standby-bucket/staging/")       │          │
│  │    .execute()                                             │          │
│  │                                                           │          │
│  │  Output:                                                  │          │
│  │    - Rewritten metadata files in staging/                 │          │
│  │    - CSV file list: [(source_path, target_path), ...]     │          │
│  └──────────────────────────────────────────────────────────┘          │
│                                                                         │
│  Step 2: COPY DATA FILES (parallel S3-to-S3 copy)                      │
│  ┌──────────────────────────────────────────────────────────┐          │
│  │  Read CSV file list from Step 1                           │          │
│  │  For each (source, target) pair:                          │          │
│  │    aws s3 cp source target  (or use Spark distributed)    │          │
│  │                                                           │          │
│  │  Only copies files REFERENCED by Iceberg snapshots        │          │
│  │  (no orphan files, no expired files — clean replication)  │          │
│  └──────────────────────────────────────────────────────────┘          │
│                                                                         │
│  Step 3: REGISTER IN STANDBY CATALOG                                   │
│  ┌──────────────────────────────────────────────────────────┐          │
│  │  standby_catalog.registerTable(                           │          │
│  │      TableIdentifier.of("finance", "transactions"),       │          │
│  │      "s3://standby-bucket/staging/v5.metadata.json"       │          │
│  │  )                                                        │          │
│  │                                                           │          │
│  │  Table is now queryable in standby region!                │          │
│  └──────────────────────────────────────────────────────────┘          │
└────────────────────────────────────────────────────────────────────────┘
```

**Key Iceberg APIs used:**

1. **`RewriteTablePathSparkAction`** (`spark/.../actions/RewriteTablePathSparkAction.java`):
   - High-level Spark action that orchestrates the entire metadata rewrite
   - `rewriteLocationPrefix(src, dst)` — rewrites all S3 paths in metadata
   - `startVersion` / `endVersion` — **incremental replication** (only process new snapshots)
   - `stagingLocation` — where to write rewritten metadata
   - `execute()` → returns staging location + CSV file list of all files to copy

2. **`RewriteTablePathUtil`** (`core/.../RewriteTablePathUtil.java`):
   - Core utility called by the Spark action
   - `replacePaths(metadata, srcPrefix, dstPrefix)` — rewrites ALL path references in TableMetadata:
     - Snapshot manifest list locations
     - Data file paths within manifests
     - Delete file paths within manifests
     - Position delete file paths and deletion vector blob metadata
     - Statistics file locations
     - Metadata log entries
   - `rewriteManifestList()`, `rewriteDataManifest()`, `rewriteDeleteManifest()` — per-level rewrites
   - Returns `RewriteResult<T>` containing both rewritten files AND copy plan (source→target mapping)

3. **`Catalog.registerTable()`** (`api/.../catalog/Catalog.java`):
   - `registerTable(TableIdentifier, metadataFileLocation)` — registers an existing metadata file in the catalog
   - Atomic operation — table becomes queryable immediately after registration

**Why `registerTable()` and not `SnapshotTable`?**

Iceberg has a `SnapshotTable` action (`SparkActions.get().snapshotTable()`) that sounds like it could work for DR. It doesn't. Here's why:

`SnapshotTable` creates a **new Iceberg table by importing file metadata from a Spark session catalog table** (Hive/Parquet). From `SnapshotTableSparkAction.java:46-49`:
```java
/**
 * Creates a new Iceberg table based on a source Spark table. The new Iceberg table
 * will have a different data and metadata directory allowing it to exist independently
 * of the source table.
 */
```

`registerTable` simply **points a catalog entry at an existing `metadata.json`**. Zero I/O — it's a metadata pointer update. The metadata.json and all referenced files must already exist at the target location.

| Aspect | `Catalog.registerTable()` | `SparkActions.snapshotTable()` |
|---|---|---|
| **Source type** | Any Iceberg `metadata.json` file path | **Spark session catalog only** (`spark_catalog`) — Hive/Parquet tables |
| **Iceberg-to-Iceberg** | Yes | **No** — source must be in `spark_catalog`, not an Iceberg catalog |
| **What it does** | Points catalog at existing metadata (zero I/O) | Reads source file listing, creates NEW Iceberg manifests + metadata |
| **Path rewriting** | None needed (metadata already rewritten by `RewriteTablePath`) | **None** — references original source file paths |
| **Data file independence** | Depends on your setup | Shares source data files; sets `gc.enabled=false` to prevent accidental cleanup |
| **Use case** | "I have a ready-to-go metadata.json — make the catalog aware of it" | "Convert a Hive/Parquet table into an Iceberg table" |

`SnapshotTable` has two fatal problems for cross-region DR:
1. **Only works with `spark_catalog` tables** (enforced at line 212-214: `"Cannot snapshot a table that isn't in the session catalog"`). It cannot snapshot an Iceberg table from a REST/Glue catalog.
2. **Does not rewrite file paths** — the new table's metadata still references the original data file paths in the primary region's S3 bucket. Queries in the standby region would try to read from the primary bucket (cross-region, slow, and fails during outage).

The correct 3-step flow is:
1. `RewriteTablePathSparkAction` → rewrites all paths from `s3://primary/` to `s3://standby/`
2. Copy data files → files now exist at `s3://standby/` paths
3. `Catalog.registerTable()` → standby catalog now points to the rewritten `metadata.json` with `s3://standby/` paths

Step 3 must be `registerTable` because by that point you have a complete, self-consistent `metadata.json` with all paths already pointing to the standby region. You just need the catalog to know about it. No file scanning, no manifest creation — just a pointer update.

#### How Atomic Metadata Consistency Is Achieved

The atomicity guarantee comes from **ordering and indirection**: files are copied BEFORE the
catalog pointer is updated. The catalog only learns about the new files when `registerTable()`
atomically swaps the metadata pointer. If the copy fails halfway, the catalog still points to
the old (consistent) metadata.

**The protocol — step by step:**

```
Step 1: REWRITE METADATA (in staging — NOT yet visible to catalog)
  RewriteTablePathSparkAction writes rewritten metadata to:
    s3://standby-bucket/staging/v12.metadata.json
  This file references files at s3://standby-bucket/warehouse/...
  But those files DON'T EXIST YET in standby.
  The standby catalog still points to v10.metadata.json (old, consistent).
  → Standby is readable and consistent (serving v10 data).

Step 2: COPY ALL DATA FILES from primary to standby
  Read the CSV file list from Step 1.
  Copy each file: s3://primary/... → s3://standby/...
  This may take minutes to hours depending on volume.

  If copy FAILS midway:
    → Some files exist in standby, some don't.
    → BUT the catalog still points to v10.metadata.json.
    → Standby queries still work perfectly (reading v10 files).
    → Retry the copy later. No inconsistency exposed to any reader.

Step 3: VERIFY all files are present
  For each (source, target) in the file list:
    HEAD s3://standby-bucket/warehouse/... → must return 200 OK

  If any file is missing:
    → Do NOT proceed to Step 4. Retry copy for missing files.
    → Catalog still on v10 — standby still consistent.

Step 4: ATOMIC CATALOG POINTER SWAP (registerTable)
  catalog.registerTable(identifier, "s3://standby/staging/v12.metadata.json")

  This is a single atomic operation in the catalog:
    REST Catalog:  single HTTP PUT to update metadata location
    Glue Catalog:  single UpdateTable API call
    Nessie:        single commit

  BEFORE this call: catalog → v10.metadata.json → v10 files (all present) ✓
  AFTER  this call: catalog → v12.metadata.json → v12 files (all present) ✓

  There is NO intermediate state where the catalog points to metadata
  that references files that don't exist yet.
```

**Why this works — the consistency model:**

```
┌─────────────────────────────────────────────────────────────────┐
│  CONSISTENCY MODEL                                                │
│                                                                   │
│  Catalog ──pointer──▶ metadata.json ──refs──▶ manifest list      │
│                                        ──refs──▶ manifests       │
│                                        ──refs──▶ data files      │
│                                                                   │
│  The catalog pointer is the ONLY entry point for queries.        │
│  Every query follows: catalog → metadata → manifests → files.   │
│                                                                   │
│  Key invariant:                                                  │
│    The metadata.json pointed to by the catalog MUST reference    │
│    ONLY files that exist and are complete.                       │
│                                                                   │
│  Guaranteed because:                                             │
│    1. Files are copied FIRST (Step 2)                            │
│    2. File presence is verified (Step 3)                         │
│    3. Catalog pointer is swapped LAST (Step 4)                   │
│                                                                   │
│  Concurrent reads during the swap:                               │
│    Before Step 4: all reads → v10 metadata → consistent         │
│    During Step 4: atomic swap — reader sees v10 OR v12, no mix  │
│    After Step 4:  all reads → v12 metadata → consistent         │
└─────────────────────────────────────────────────────────────────┘
```

**Failure recovery at each step:**

| Failure Point | Catalog State | Standby Queries | Recovery |
|---|---|---|---|
| Step 1 fails (metadata rewrite) | Still on v10 | Consistent (reads v10) | Retry Step 1 |
| Step 2 fails (partial file copy) | Still on v10 | Consistent (reads v10) | Retry copy for missing files |
| Step 3 fails (verification) | Still on v10 | Consistent (reads v10) | Re-copy failed files, re-verify |
| Step 4 fails (catalog update) | Still on v10 | Consistent (reads v10) | Retry registerTable |
| Step 4 succeeds | Now on v12 | Consistent (reads v12) | Done |

Every failure leaves the standby in its previous consistent state. There is no partial state
where the catalog references metadata whose files are only partially copied.

**Contrast with S3 CRR — why CRR does NOT have this atomicity:**

S3 CRR replicates files as they arrive, in arbitrary order. The `metadata.json` may arrive
BEFORE all data files it references. If a query engine in the standby region reads the new
`metadata.json` while data files are still being replicated → `FileNotFoundException`. This is
why S3 CRR requires a SEPARATE catalog sync mechanism with its own consistency protocol
(typically a periodic job that checks whether all files referenced by the latest metadata are
present before updating the standby catalog pointer).

**Incremental replication flow:**

```
Day 1: Full backup
  startVersion = v1.metadata.json (first version)
  endVersion   = v10.metadata.json (current)
  → Rewrites all metadata, generates full file list
  → Copy all referenced files (~500 TB)
  → Register table in standby catalog

Day 2: Incremental backup
  startVersion = v10.metadata.json (last backed up)
  endVersion   = v12.metadata.json (current)
  → Only rewrites metadata for snapshots 11-12
  → File list contains only NEW files (not already in standby)
  → Copy ~2 TB of new files
  → Update registration (or re-register with new metadata)
```

#### Incremental File Discovery: APIs in Detail

The `RewriteTablePathSparkAction` with `startVersion`/`endVersion` handles the metadata
side. But to understand **exactly which files changed**, Iceberg provides several APIs:

**1. `SnapshotChanges` API** (`core/.../SnapshotChanges.java`) — Per-snapshot file diff:

```java
// Get files added and removed in a specific snapshot
SnapshotChanges changes = SnapshotChanges.builderFor(table)
    .snapshot(table.snapshot(snapshotId))
    .build();

Iterable<DataFile> added = changes.addedDataFiles();      // new files in this snapshot
Iterable<DataFile> removed = changes.removedDataFiles();   // files deleted in this snapshot
Iterable<DeleteFile> addedDeletes = changes.addedDeleteFiles();
Iterable<DeleteFile> removedDeletes = changes.removedDeleteFiles();
```

Each file is a `DataFile` / `DeleteFile` with full metadata: `path()`, `fileSizeInBytes()`, `recordCount()`, `partition()`.

**2. `ManifestEntry.Status`** — The underlying tracking mechanism:

Every file in every manifest has a status:
```
EXISTING (0)  — file existed before this snapshot and still exists (carried forward)
ADDED    (1)  — file was newly added in this snapshot
DELETED  (2)  — file was removed in this snapshot
```

**3. `Snapshot.summary()`** — Quick counts without reading manifests:

```java
Map<String, String> summary = snapshot.summary();
summary.get("added-data-files");     // e.g., "64"
summary.get("deleted-data-files");   // e.g., "0"
summary.get("added-files-size");     // e.g., "1073741824" (bytes)
summary.get("removed-files-size");   // e.g., "0"
```

Useful for quick RPO monitoring: check if new snapshots have appeared without reading full manifests.

**4. `Snapshot.operation()`** — What type of change happened:

```
"append"    — new data added, nothing deleted (normal ingestion)
"overwrite" — data added AND deleted (e.g., MERGE INTO, partition overwrite)
"delete"    — only deletions (e.g., DELETE FROM)
"replace"   — files rewritten without logical data change (COMPACTION)
```

**5. `IncrementalAppendScan`** — Scan across a range of snapshots (append-only):

```java
// Get all files added between two snapshots (only appends, skips replaces)
IncrementalAppendScan scan = table.newIncrementalAppendScan()
    .fromSnapshotExclusive(lastReplicatedSnapshotId)
    .toSnapshot(currentSnapshotId);

for (FileScanTask task : scan.planFiles()) {
    DataFile file = task.file();
    // This file needs to be copied to standby
}
```

**6. SQL metadata table** — `all_entries` for ad-hoc investigation:

```sql
-- All file changes between snapshot 100 and 200
SELECT status, snapshot_id, file_path, file_size_in_bytes, record_count
FROM catalog.finance.transactions.all_entries
WHERE snapshot_id > 100 AND snapshot_id <= 200
  AND (status = 1 OR status = 2)  -- ADDED or DELETED
ORDER BY snapshot_id, status
```

#### What Happens When Compaction Runs?

Compaction (`rewrite_data_files`) creates a snapshot with **`operation = "replace"`**. This is the
most complex case for incremental replication:

```
┌────────────────────────────────────────────────────────────────────────┐
│  COMPACTION SNAPSHOT (operation = "replace")                             │
│                                                                         │
│  Before compaction (snapshot 10):                                      │
│    file_001.parquet (15 MB)  ← small file                             │
│    file_002.parquet (15 MB)  ← small file                             │
│    file_003.parquet (15 MB)  ← small file                             │
│    ... (64 small files from streaming)                                 │
│                                                                         │
│  After compaction (snapshot 11, operation="replace"):                   │
│    DELETED: file_001.parquet, file_002.parquet, ... (64 files)         │
│    ADDED:   file_compact_001.parquet (256 MB)  ← compacted file       │
│                                                                         │
│  SnapshotChanges for snapshot 11:                                      │
│    addedDataFiles()   → [file_compact_001.parquet]   (1 new file)     │
│    removedDataFiles() → [file_001..file_064.parquet] (64 old files)   │
│                                                                         │
│  Replication impact:                                                   │
│    - Must copy: file_compact_001.parquet (the new compacted file)     │
│    - Can skip: file_001..file_064 (already in standby from earlier)   │
│    - Standby metadata must be updated to reflect the replace          │
│    - Old files in standby become orphans after metadata update        │
│      → cleaned up by standby's orphan file removal                    │
└────────────────────────────────────────────────────────────────────────┘
```

**Three replication strategies for handling compaction:**

**Strategy 1: Replicate everything (simple, correct, wasteful)**
```
For each snapshot between lastReplicated and current:
  Copy ALL addedDataFiles to standby (includes compacted files)
  Update standby metadata to include all snapshots

Pro: simple, always correct
Con: copies compacted files even though the original small files are already in standby
     (those small files will become orphans in standby — wasted storage until cleanup)
```

**Strategy 2: Skip REPLACE snapshots (efficient, limited)**
```
For each snapshot between lastReplicated and current:
  IF snapshot.operation() == "replace":
    SKIP — don't replicate compaction results
  ELSE:
    Copy addedDataFiles, update metadata

Pro: avoids replicating compaction output (saves bandwidth)
Con: standby has uncompacted files → worse read performance in standby
     standby must run its OWN compaction → extra compute cost in standby
```

**Strategy 3: Replicate only added files, compact independently (recommended)**
```
For each snapshot between lastReplicated and current:
  Copy ALL addedDataFiles (regardless of operation type)
  Update standby metadata with all snapshots

Then: run compaction in standby region on its own schedule

Pro: standby has all data + can optimize independently
Con: slightly more complex (two compaction schedules)
     standby storage temporarily higher (old + compacted files until cleanup)
```

**`RewriteTablePathSparkAction` handles this correctly:**
The `startVersion`/`endVersion` approach works at the **metadata version level**, not individual
snapshots. It rewrites ALL metadata between versions — including compaction snapshots. The
generated file list includes ALL files referenced by the end version that weren't in the start
version. This naturally handles compaction: compacted files appear in the file list (they're new),
and the rewritten metadata correctly reflects the replace operation.

```
RewriteTablePath with startVersion=v10, endVersion=v12:

  v10 metadata references: [file_001, file_002, ..., file_064, file_A, file_B]
  v12 metadata references: [file_compact_001, file_A, file_B, file_C]
                            (compaction replaced 64 files with 1; file_C is new append)

  File list to copy = files_in_v12 - files_in_v10
                    = [file_compact_001, file_C]
                    (exactly the new files — both the compacted file and the new append)

  Files no longer needed = files_in_v10 - files_in_v12
                         = [file_001, ..., file_064]
                         (these are orphans in standby after metadata update)
```

---

### Alternative C: Snapshot Replay Replication (Cherry-Pick Inspired)

**How it works:** Instead of rewriting entire metadata files (Alternative B), replay each
snapshot's changes individually into the standby table — exactly like Iceberg's `cherry_pick_snapshot`
replays a WAP snapshot onto the current branch. For each primary snapshot: enumerate changed
files via `SnapshotChanges`, copy only those files to standby, then commit a matching snapshot
in the standby table using `newAppend()` / `newOverwrite()`.

This is the most **granular** approach: the standby table maintains **its own snapshot history**
that mirrors the primary's, snapshot by snapshot.

```
┌────────────────────────────────────────────────────────────────────────┐
│  ALTERNATIVE C: SNAPSHOT REPLAY REPLICATION                             │
│                                                                         │
│  PRIMARY TABLE (us-east-1)           STANDBY TABLE (eu-west-1)        │
│  ┌──────────────────────┐            ┌──────────────────────┐         │
│  │ Snap 1 (append)      │            │                      │         │
│  │  + file_001.parquet  │──copy──▶   │ Snap 1' (append)     │         │
│  │  + file_002.parquet  │  files     │  + file_001.parquet   │         │
│  ├──────────────────────┤            ├──────────────────────┤         │
│  │ Snap 2 (overwrite)   │            │                      │         │
│  │  - file_001.parquet  │──copy──▶   │ Snap 2' (overwrite)  │         │
│  │  + file_003.parquet  │  new file  │  - file_001.parquet   │         │
│  ├──────────────────────┤            ├──────────────────────┤         │
│  │ Snap 3 (append)      │            │                      │         │
│  │  + file_004.parquet  │──copy──▶   │ Snap 3' (append)     │         │
│  └──────────────────────┘  file      │  + file_004.parquet   │         │
│                                       └──────────────────────┘         │
│                                                                         │
│  Each primary snapshot is "replayed" as a new commit in the standby.  │
│  Standby has its OWN snapshot IDs but mirrors the same logical changes.│
└────────────────────────────────────────────────────────────────────────┘
```

**The core algorithm (inspired by `CherryPickOperation.java`):**

```
For each unreplicated snapshot on the primary (oldest to newest):

  1. ENUMERATE CHANGES using SnapshotChanges API
     SnapshotChanges changes = SnapshotChanges.builderFor(primaryTable)
         .snapshot(primarySnapshot)
         .build();
     List<DataFile> added = changes.addedDataFiles();
     List<DataFile> removed = changes.removedDataFiles();
     List<DeleteFile> addedDeletes = changes.addedDeleteFiles();

  2. COPY NEW FILES to standby S3
     For each file in added + addedDeletes:
       Copy from s3://primary/... to s3://standby/...
       Create replica DataFile with standby path:
         DataFiles.builder(spec).copy(sourceFile).withPath(standbyPath).build()

  3. REPLAY THE OPERATION based on snapshot.operation()

     If operation == "append":
       standbyTable.newAppend()
           .appendFile(replicaFile1)
           .appendFile(replicaFile2)
           .commit()

     If operation == "overwrite" or "delete":
       OverwriteFiles overwrite = standbyTable.newOverwrite();
       for (DataFile f : replicaAdded):  overwrite.addFile(f);
       for (DataFile f : replicaRemoved): overwrite.deleteFile(f);
       overwrite.commit()

     If operation == "replace" (compaction):
       RewriteFiles rewrite = standbyTable.newRewrite();
       rewrite.rewriteFiles(replicaRemoved, replicaAdded);
       rewrite.commit()

  4. RECORD the primary snapshot ID as "last replicated"
     (Store in a checkpoint file or table property)
```

**Key Iceberg APIs used:**

1. **`SnapshotChanges`** (`core/.../SnapshotChanges.java`) — Enumerates exactly which files
   were added/removed in a single snapshot. This is the same API used internally by
   `CherryPickOperation`:
   ```java
   SnapshotChanges changes = SnapshotChanges.builderFor(table)
       .snapshot(snapshot)
       .executeWith(executorService)  // parallel manifest reading
       .build();
   changes.addedDataFiles();      // files with ManifestEntry.Status.ADDED
   changes.removedDataFiles();    // files with ManifestEntry.Status.DELETED
   changes.addedDeleteFiles();    // MOR delete files added
   changes.removedDeleteFiles();  // MOR delete files removed
   ```

2. **`Snapshot.operation()`** — Returns the operation type (`DataOperations.java`):
   - `"append"` → new data, no deletions → replay with `newAppend()`
   - `"overwrite"` → data added AND deleted (MERGE INTO, partition overwrite) → replay with `newOverwrite()`
   - `"delete"` → only deletions → replay with `newOverwrite()` (delete-only)
   - `"replace"` → compaction (file rewrite, no logical change) → replay with `newRewrite()` or skip

3. **`DataFiles.builder(spec).copy(sourceFile).withPath(newPath).build()`** — Creates a replica
   `DataFile` object with the standby path but preserving all metadata (partition values,
   record count, file size, column stats):
   ```java
   DataFile replicaFile = DataFiles.builder(spec)
       .copy(primaryFile)                          // copy all metadata
       .withPath(primaryFile.path().toString()
           .replace("s3://primary/", "s3://standby/"))  // change path
       .build();
   ```

4. **`AppendFiles` / `OverwriteFiles` / `RewriteFiles`** — Commit APIs that create new
   snapshots in the standby table:
   ```java
   // Replay an append
   standbyTable.newAppend().appendFile(replicaFile).commit();

   // Replay an overwrite (MERGE INTO, DELETE)
   OverwriteFiles op = standbyTable.newOverwrite();
   addedFiles.forEach(op::addFile);
   removedFiles.forEach(op::deleteFile);
   op.commit();

   // Replay compaction (optional — can also skip)
   standbyTable.newRewrite()
       .rewriteFiles(removedFileSet, addedFileSet)
       .commit();
   ```

5. **`SnapshotUtil.newFilesBetween()`** (`core/.../util/SnapshotUtil.java`) — For bulk
   incremental: get ALL new files between two snapshots at once (less granular but faster):
   ```java
   CloseableIterable<DataFile> newFiles = SnapshotUtil.newFilesBetween(
       lastReplicatedSnapshotId,
       currentPrimarySnapshotId,
       primaryTable::snapshot,
       primaryTable.io());
   ```

**How compaction is handled:**

Compaction snapshots have `operation = "replace"`. Three options:

```
Option 1: REPLAY compaction in standby (recommended)
  Use newRewrite() to replace old files with compacted files.
  Standby gets the same optimized file layout as primary.
  Cost: must copy compacted files to standby.

Option 2: SKIP compaction snapshots
  Don't replay "replace" operations.
  Standby retains uncompacted files → worse read performance.
  Standby runs its own compaction independently.
  Cost: saves replication bandwidth, adds standby compute.

Option 3: COMPACT INDEPENDENTLY in standby
  Replay only append/overwrite/delete snapshots.
  Run separate compaction jobs in standby on its own schedule.
  Standby may have different file layout than primary.
```

**Why this is better than cherry-pick for cross-region:**

Iceberg's built-in `cherry_pick_snapshot` operates on a SINGLE table — it replays a snapshot
from one branch onto another branch of the SAME table. It cannot:
- Copy files across S3 buckets / regions
- Rewrite file paths
- Operate across two different catalog entries

The Snapshot Replay approach uses the same underlying primitives (`SnapshotChanges`,
`add(DataFile)`, `delete(DataFile)`) but adds the file-copy and path-rewriting layer needed
for cross-region operation.

**Comparison with Alternative B (RewriteTablePath):**

| Aspect | B: RewriteTablePath | C: Snapshot Replay |
|---|---|---|
| **Granularity** | Metadata version level | Individual snapshot level |
| **Standby snapshot history** | Single metadata swap (history not preserved) | Full snapshot-by-snapshot history mirrored |
| **Time travel in standby** | Only sees final state | Can time-travel through replayed snapshots |
| **Incremental cost** | Rewrites ALL metadata between versions | Only processes changed files per snapshot |
| **Compaction handling** | Automatic (file diff between versions) | Explicit (choose to replay, skip, or compact independently) |
| **Complexity** | Medium (single Spark action) | High (per-snapshot loop with operation dispatch) |
| **Standby table independence** | Registered from external metadata | True Iceberg table with own commit history |
| **Conflict handling** | None (metadata overwrite) | Uses Iceberg's optimistic concurrency |

---

### Comparison: All Three Replication Alternatives

| Aspect | A: S3 CRR | B: RewriteTablePath | C: Snapshot Replay |
|---|---|---|---|
| **Setup complexity** | Low | Medium | High |
| **Automation** | Fully automatic | Scheduled job | Scheduled job (more logic) |
| **Granularity** | S3 prefix level | Metadata version | Per-snapshot |
| **Standby history** | N/A (file copy only) | Latest state only | Full snapshot history |
| **Time travel in standby** | Depends on catalog sync | Limited | Full |
| **Cross-cloud** | AWS only | Any cloud | Any cloud |
| **RPO** | ~15 min (RTC SLA) | Job schedule dependent | Job schedule dependent |
| **What gets copied** | All objects (incl. orphans) | Files referenced by end version | Only per-snapshot changed files |
| **Metadata consistency** | Separate catalog sync needed | Atomic (registerTable) | Atomic (per-commit) |
| **Compaction handling** | Copies all files blindly | Diffs versions (handles automatically) | Explicit control (replay/skip/independent) |
| **Orphan files** | Replicated (waste) | Not replicated | Not replicated |
| **Standby independence** | File mirror only | Registered metadata | True independent table |
| **Cost (2 TB/day)** | ~$1,200/month (transfer) | ~$200/month (compute) | ~$250/month (compute, more API calls) |
| **Best for** | Simple AWS-only DR | Bulk metadata sync | Granular incremental DR with full history |

**When to choose each:**

- **S3 CRR (A)**: Simple AWS-only setup, low operational overhead, guaranteed RPO via SLA
- **RewriteTablePath (B)**: Multi-cloud, bulk sync, don't need snapshot history in standby
- **Snapshot Replay (C)**: Need full snapshot history in standby (time travel, audit), want
  explicit control over compaction handling, building a production-grade replication service

**Production recommendation:**
Start with **B (RewriteTablePath)** for initial deployment — simpler, well-tested Iceberg action.
Evolve to **C (Snapshot Replay)** when you need standby time travel, audit compliance, or
per-snapshot RPO tracking. Use **A (S3 CRR)** as a complementary safety net for data durability.

---

## 4. Deep Dive

### 4.1 Schema Design

```sql
-- Example: financial transactions table replicated across regions
CREATE TABLE catalog.finance.transactions (
    txn_id          BIGINT,
    account_id      BIGINT,
    txn_type        STRING,      -- debit, credit, transfer, fee
    amount          DECIMAL(15,2),
    currency        STRING,
    merchant_id     BIGINT,
    merchant_category STRING,
    txn_status      STRING,      -- pending, completed, failed, reversed
    region          STRING,      -- us, eu, apac
    country         STRING,
    event_time      TIMESTAMP,
    processed_at    TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_time), region)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
```

**DR-specific schema considerations:**
- `region` column enables filtering for data residency compliance
- If EU data must stay in EU: create separate tables (`transactions_eu`, `transactions_us`) or use row-level security
- `processed_at` tracks when data was committed — useful for verifying replication completeness

### 4.2 Partition Design

```
PARTITIONED BY (days(event_time), region)
```

**Analysis:**
- Daily partition at 2 TB/day ÷ 3 regions = ~667 GB/region/day → large but acceptable for batch
- For finer granularity: `hours(event_time)` would give ~28 GB/region/hour
- `region` partition enables **selective replication**: only replicate non-regulated regions fully; EU data stays in EU

**Cross-region replication & partitions:**
- S3 CRR replicates by object prefix → partition layout maps naturally to S3 prefixes
- Partition-level replication monitoring: verify each partition's files are present in replica

### 4.3 Sort Order Design

```sql
ALTER TABLE catalog.finance.transactions WRITE ORDERED BY account_id, event_time
```

**Effective sort order:** `[days(event_time) ASC, region ASC, account_id ASC, event_time ASC]`

**Why:**
- Most queries filter by account: `WHERE account_id = X AND event_time > Y`
- Sort by `account_id` within each partition → tight min/max ranges → file pruning skips 95%+ of files
- `event_time` as secondary sort → range queries within an account are sequential reads

### 4.4 Write Distribution Mode

```
write.distribution-mode = range   (from WRITE ORDERED BY)
```

**Analysis for DR context:**
- Range distribution ensures globally sorted files → optimal for read performance in both regions
- Both primary and standby serve identical file layouts → query performance is symmetric
- **Critical for failover**: standby region gets the same well-organized files, no performance degradation on failover

**Alternative — HASH for write throughput:**
- If write latency is more critical than read performance: `WRITE DISTRIBUTED BY PARTITION ORDERED BY account_id`
- Sets mode=HASH → cheaper shuffle at write time
- Sort is local only → files have overlapping account_id ranges → less effective pruning
- For financial services (read-heavy reporting), RANGE is preferred

### 4.5 Read/Write Performance Analysis

**Primary region (normal operation):**
```
Write: Spark batch job, 2 TB/day, RANGE distribution
  → well-sized files (256 MB), globally sorted by account_id
  → write completes in ~2 hours with 100 executors

Read: Trino dashboards
  → partition pruning: days(event_time) eliminates 99%+ of partitions
  → file pruning: account_id sort order eliminates 95% of files within partition
  → typical query: 100ms planning, 2-5 sec execution
```

**Standby region (normal operation — read-only):**
```
Read: Trino connected to replica catalog
  → same file layout as primary (S3 CRR preserves file structure)
  → performance identical to primary for read workloads
  → replication lag: queries may miss last 10-15 min of data
```

**Standby region (after failover):**
```
Write: Spark writers started in standby region
  → writes to standby S3 bucket (now the new primary)
  → catalog promoted to writable
  → no performance degradation — same file sizes, same sort order
  → first few commits may overlap with last replicated data → dedup via order_id
```

### 4.6 Maintenance Jobs

```
┌────────────────────────────────────────────────────────────────────────┐
│  MAINTENANCE — RUNS IN PRIMARY REGION ONLY                              │
│  (Standby receives maintenance results via replication)                 │
│                                                                         │
│  DAILY: Compaction (sort strategy)                                     │
│    CALL catalog.system.rewrite_data_files(                             │
│      table => 'finance.transactions',                                  │
│      strategy => 'sort',                                               │
│      where => 'event_time >= current_date() - INTERVAL 2 DAYS'        │
│    )                                                                   │
│    → Compacted files replicate to standby automatically via S3 CRR    │
│    → Old files cleaned up after snapshot expiration + orphan removal   │
│                                                                         │
│  DAILY: Expire snapshots                                               │
│    CALL catalog.system.expire_snapshots(                               │
│      table => 'finance.transactions',                                  │
│      older_than => TIMESTAMP 'now - 7 days',                           │
│      retain_last => 50                                                 │
│    )                                                                   │
│    ⚠ IMPORTANT FOR DR: snapshot expiration deletes old metadata        │
│    → standby must sync BEFORE expiration runs                          │
│    → sequence: sync catalog → expire snapshots → sync again           │
│                                                                         │
│  WEEKLY: Remove orphan files + rewrite manifests                       │
│    → Same as single-region, but verify orphan removal doesn't          │
│      delete files still being replicated (use conservative age: 7d)   │
│                                                                         │
│  DR-SPECIFIC: Replication health check (every 10 min)                  │
│    → Compare latest snapshot ID in primary vs standby catalog          │
│    → Alert if lag exceeds RPO (15 min)                                 │
│    → Verify S3 replication metrics (pending bytes, failed objects)     │
└────────────────────────────────────────────────────────────────────────┘
```

### 4.7 Schema Evolution & Partition Evolution Impact

**Schema Evolution in DR context:**
- Schema changes in primary propagate via metadata replication
- `ALTER TABLE ADD COLUMN` updates `metadata.json` → replicated to standby
- **Timing risk**: if failover occurs during schema evolution, standby may have old schema
  - Mitigation: catalog sync must be verified before promoting standby
  - New data files with new schema columns won't match old metadata → use Iceberg's field-ID tracking for safety

**Partition Evolution in DR context:**
- `ALTER TABLE ADD PARTITION FIELD hours(event_time)` creates new partition spec
- New files use new spec, old files keep old spec → Iceberg handles transparently
- **Replication impact**: new partition layout → new S3 prefixes → S3 CRR automatically covers them
- **Compaction caveat**: after partition evolution, don't compact files across different specs
  - This applies equally in primary and standby

**Catalog sync during evolution:**
- Both schema and partition evolution update `metadata.json`
- The metadata pointer in the catalog must be atomically updated in both regions
- If using REST catalog replication: the catalog sync service must replicate the latest metadata pointer
- If using S3 CRR only (no catalog sync): engines in standby must read `metadata.json` directly from S3

---

## 5. Further Improvements

**Additional requirement questions:**
- Is cross-region query routing needed? (route EU users to EU region)
- What's the compliance audit trail for failover events?
- Are there write-write conflict scenarios? (active-active future plan)
- What's the network bandwidth between regions? (affects replication lag)
- Is there a need for partial replication? (only replicate specific tables)

**Advanced topics:**
- **Active-Active Architecture**: Both regions accept writes to different tables/partitions. Requires conflict resolution (e.g., partition-level ownership). Much more complex.
- **Catalog-Level Replication (Nessie)**: Nessie's built-in replication can sync catalog state including branches and tags. More efficient than custom catalog sync.
- **AWS S3 Tables Replication**: AWS native Iceberg replication creates read-only replicas that maintain snapshot ordering and automatically rewrite file paths for the destination bucket.
- **Uber's HiveSync Pattern**: Sharded, event-driven replication system. Handles 5M daily events, 8PB replication, 99.99% accuracy. Uses hybrid RPC + DistCp strategies and DAG-based orchestration.
- **Zero-RPO with synchronous commit**: Write to both regions before acknowledging commit. Doubles write latency but guarantees no data loss. Suitable for critical financial data.
- **Tiered DR**: Tier-1 tables (critical) get synchronous replication; Tier-2 get async; Tier-3 get daily batch copy.

---

## Appendix: Spark + Iceberg Code Examples

### A1. Create table with DR-friendly properties

```python
spark.sql("""
CREATE TABLE catalog.finance.transactions (
    txn_id          BIGINT,
    account_id      BIGINT,
    txn_type        STRING,
    amount          DECIMAL(15,2),
    currency        STRING,
    merchant_id     BIGINT,
    merchant_category STRING,
    txn_status      STRING,
    region          STRING,
    country         STRING,
    event_time      TIMESTAMP,
    processed_at    TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_time), region)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'history.expire.max-snapshot-age-ms' = '604800000',  -- 7 days
    'commit.retry.num-retries' = '10'
)
""")

spark.sql("""
ALTER TABLE catalog.finance.transactions
WRITE ORDERED BY account_id, event_time
""")
```

### A2. Replication health check

```python
# Compare snapshots between primary and standby
primary_snapshot = spark.sql(
    "SELECT snapshot_id, committed_at FROM catalog.finance.transactions.snapshots ORDER BY committed_at DESC LIMIT 1"
).collect()[0]

standby_snapshot = spark.sql(
    "SELECT snapshot_id, committed_at FROM standby_catalog.finance.transactions.snapshots ORDER BY committed_at DESC LIMIT 1"
).collect()[0]

lag_minutes = (primary_snapshot.committed_at - standby_snapshot.committed_at).total_seconds() / 60
if lag_minutes > 15:
    alert(f"DR replication lag exceeds RPO: {lag_minutes:.1f} minutes")
```

### A3. Failover procedure

```python
# Step 1: Verify standby data completeness
latest_standby = spark.sql(
    "SELECT MAX(event_time) as latest FROM standby_catalog.finance.transactions"
).collect()[0].latest
print(f"Standby data current as of: {latest_standby}")

# Step 2: Promote standby catalog (application-level switch)
# Update catalog URI in Spark/Trino configs to point to standby region
# This is typically done via DNS switch or config management

# Step 3: Verify write capability
spark.sql("""
INSERT INTO standby_catalog.finance.transactions
VALUES (0, 0, 'healthcheck', 0.00, 'USD', 0, 'test', 'completed', 'us', 'US', current_timestamp(), current_timestamp())
""")
spark.sql("DELETE FROM standby_catalog.finance.transactions WHERE txn_type = 'healthcheck'")
print("Failover verified: standby is writable")
```

### A4. Iceberg API: Full backup using RewriteTablePath

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.primary", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.primary.type", "rest") \
    .config("spark.sql.catalog.primary.uri", "http://primary-catalog:8181") \
    .config("spark.sql.catalog.standby", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.standby.type", "rest") \
    .config("spark.sql.catalog.standby.uri", "http://standby-catalog:8181") \
    .getOrCreate()

# Load the source table
table = spark.catalog("primary").loadTable("finance.transactions")

# Step 1: Rewrite all metadata paths from primary to standby bucket
from org.apache.iceberg.spark.actions import SparkActions

result = SparkActions.get(spark) \
    .rewriteTablePath(table) \
    .rewriteLocationPrefix(
        "s3://primary-bucket/warehouse",
        "s3://standby-bucket/warehouse"
    ) \
    .stagingLocation("s3://standby-bucket/staging/finance/transactions/") \
    .execute()

print(f"Staging location: {result.stagingLocation()}")
print(f"File list: {result.fileListLocation()}")
# file list is a CSV with columns: source_path, target_path
```

### A5. Iceberg API: Copy files from file list

```python
# Step 2: Read the file list and copy files in parallel using Spark
file_list_df = spark.read.csv(result.fileListLocation(), header=True)
# Columns: source_path, target_path

# Option A: Use aws s3 cp (for S3-to-S3, server-side copy is fast)
import subprocess

def copy_file(row):
    subprocess.run(["aws", "s3", "cp", row.source_path, row.target_path, "--quiet"])

file_list_df.foreach(copy_file)  # Distributed copy across Spark executors

# Option B: For cross-cloud (S3 → GCS), use Spark read/write:
# for each file: spark.read.parquet(source).write.parquet(target)
```

### A6. Iceberg API: Register table in standby catalog

```python
# Step 3: Register the rewritten metadata in standby catalog
from org.apache.iceberg.catalog import TableIdentifier

standby_catalog = spark.catalog("standby")

# The staging location contains the rewritten metadata.json
metadata_location = f"{result.stagingLocation()}/v5.metadata.json"

# Register (or re-register for incremental updates)
try:
    standby_catalog.registerTable(
        TableIdentifier.of("finance", "transactions"),
        metadata_location
    )
    print("Table registered in standby catalog")
except Exception as e:
    # Table already exists — drop and re-register, or use updateTableMetadataLocation
    standby_catalog.dropTable(TableIdentifier.of("finance", "transactions"), False)  # purge=False
    standby_catalog.registerTable(
        TableIdentifier.of("finance", "transactions"),
        metadata_location
    )
    print("Table re-registered in standby catalog")
```

### A7. Iceberg API: Incremental backup (daily)

```python
# For daily incremental backup, specify start and end versions
# to only process new snapshots since last backup

result = SparkActions.get(spark) \
    .rewriteTablePath(table) \
    .rewriteLocationPrefix(
        "s3://primary-bucket/warehouse",
        "s3://standby-bucket/warehouse"
    ) \
    .startVersion("v10.metadata.json")  # last backed-up version
    .endVersion("v12.metadata.json")    # current version
    .stagingLocation("s3://standby-bucket/staging/finance/transactions/") \
    .execute()

# File list only contains NEW files not previously copied
# Copy them and update the standby registration
```

### A8. Iceberg API: Enumerate snapshot files for custom backup

```java
// Low-level Java API: enumerate all files referenced by a snapshot
// Useful for building custom backup tools

import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;

Table table = catalog.loadTable(TableIdentifier.of("finance", "transactions"));
Snapshot snapshot = table.currentSnapshot();
FileIO io = table.io();

// Get all manifest files in current snapshot
List<ManifestFile> allManifests = snapshot.allManifests(io);

// For each manifest, read the data files
for (ManifestFile manifest : allManifests) {
    ManifestReader<DataFile> reader = ManifestFiles.read(
        manifest, io, table.specs());
    for (DataFile dataFile : reader) {
        String filePath = dataFile.path().toString();
        long fileSize = dataFile.fileSizeInBytes();
        // Copy this file to standby region
        System.out.printf("Copy: %s (%d bytes)%n", filePath, fileSize);
    }
}

// Also copy the manifest list itself
String manifestListPath = snapshot.manifestListLocation();
// And metadata.json
String metadataPath = ((HasTableOperations) table)
    .operations().current().metadataFileLocation();
```

### A9. Post-failover: replicate delta back to primary

```python
# After primary is restored, find data written to standby during outage
outage_start = "2026-03-23T10:00:00"
outage_end = "2026-03-23T11:30:00"

# Read data written during outage from standby
delta_df = spark.read.format("iceberg") \
    .option("start-snapshot-id", pre_outage_snapshot_id) \
    .load("standby_catalog.finance.transactions")

# Write delta to restored primary
delta_df.writeTo("catalog.finance.transactions").append()
```

### A10. S3 CRR: AWS CLI configuration

```bash
# Enable versioning on both buckets
aws s3api put-bucket-versioning \
    --bucket primary-bucket \
    --versioning-configuration Status=Enabled

aws s3api put-bucket-versioning \
    --bucket standby-bucket \
    --versioning-configuration Status=Enabled

# Create replication configuration with RTC
cat > replication.json << 'EOF'
{
    "Role": "arn:aws:iam::ACCOUNT:role/s3-replication-role",
    "Rules": [
        {
            "ID": "iceberg-warehouse-replication",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "warehouse/"
            },
            "Destination": {
                "Bucket": "arn:aws:s3:::standby-bucket",
                "StorageClass": "STANDARD",
                "ReplicationTime": {
                    "Status": "Enabled",
                    "Time": { "Minutes": 15 }
                },
                "Metrics": {
                    "Status": "Enabled",
                    "EventThreshold": { "Minutes": 15 }
                }
            },
            "DeleteMarkerReplication": {
                "Status": "Disabled"
            }
        }
    ]
}
EOF

aws s3api put-bucket-replication \
    --bucket primary-bucket \
    --replication-configuration file://replication.json

# For existing data (initial 500 TB sync), use Batch Replication:
# Create a Batch Operations job in the S3 console targeting the source bucket
```

### A11. Snapshot Replay: Full replication service (Java)

```java
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

/**
 * Incremental cross-region replication using snapshot replay.
 * Inspired by CherryPickOperation — uses SnapshotChanges to enumerate
 * per-snapshot file changes, copies files, then replays changes.
 */
public class SnapshotReplayReplicator {

    private final Table primaryTable;
    private final Table standbyTable;
    private final String primaryPrefix;   // "s3://primary-bucket/warehouse"
    private final String standbyPrefix;   // "s3://standby-bucket/warehouse"
    private long lastReplicatedSnapshotId;

    // ... constructor ...

    public void replicateIncremental() {
        primaryTable.refresh();
        standbyTable.refresh();

        // Find all unreplicated snapshots (oldest to newest)
        List<Snapshot> unreplicated = findUnreplicatedSnapshots();

        for (Snapshot primarySnap : unreplicated) {
            replaySnapshot(primarySnap);
            lastReplicatedSnapshotId = primarySnap.snapshotId();
            saveCheckpoint(lastReplicatedSnapshotId);
        }
    }

    private void replaySnapshot(Snapshot primarySnap) {
        String operation = primarySnap.operation();

        // Step 1: Enumerate changes
        SnapshotChanges changes = SnapshotChanges.builderFor(primaryTable)
            .snapshot(primarySnap)
            .build();

        List<DataFile> addedFiles = Lists.newArrayList(changes.addedDataFiles());
        List<DataFile> removedFiles = Lists.newArrayList(changes.removedDataFiles());
        List<DeleteFile> addedDeletes = Lists.newArrayList(changes.addedDeleteFiles());

        // Step 2: Copy new files to standby region
        List<DataFile> replicaAdded = new ArrayList<>();
        for (DataFile file : addedFiles) {
            String standbyPath = file.path().toString()
                .replace(primaryPrefix, standbyPrefix);
            copyFile(file.path().toString(), standbyPath);  // S3 copy
            replicaAdded.add(DataFiles.builder(primaryTable.spec())
                .copy(file)
                .withPath(standbyPath)
                .build());
        }

        List<DataFile> replicaRemoved = new ArrayList<>();
        for (DataFile file : removedFiles) {
            String standbyPath = file.path().toString()
                .replace(primaryPrefix, standbyPrefix);
            replicaRemoved.add(DataFiles.builder(primaryTable.spec())
                .copy(file)
                .withPath(standbyPath)
                .build());
        }

        // Step 3: Replay the operation into standby table
        switch (operation) {
            case DataOperations.APPEND:
                AppendFiles append = standbyTable.newAppend();
                replicaAdded.forEach(append::appendFile);
                append.commit();
                break;

            case DataOperations.OVERWRITE:
            case DataOperations.DELETE:
                OverwriteFiles overwrite = standbyTable.newOverwrite();
                replicaAdded.forEach(overwrite::addFile);
                replicaRemoved.forEach(overwrite::deleteFile);
                overwrite.commit();
                break;

            case DataOperations.REPLACE:
                // Compaction — replay or skip
                RewriteFiles rewrite = standbyTable.newRewrite();
                rewrite.rewriteFiles(
                    new HashSet<>(replicaRemoved),
                    new HashSet<>(replicaAdded));
                rewrite.commit();
                break;
        }
    }
}
```

### A12. Snapshot Replay: Spark-based replication job (Python)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.primary", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.primary.type", "rest") \
    .config("spark.sql.catalog.primary.uri", "http://primary-catalog:8181") \
    .config("spark.sql.catalog.standby", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.standby.type", "rest") \
    .config("spark.sql.catalog.standby.uri", "http://standby-catalog:8181") \
    .getOrCreate()

# Step 1: Find unreplicated snapshots
primary_snapshots = spark.sql("""
    SELECT snapshot_id, operation, committed_at, summary
    FROM primary.finance.transactions.snapshots
    ORDER BY committed_at
""").collect()

last_replicated = get_checkpoint()  # read from checkpoint file/table

unreplicated = [s for s in primary_snapshots if s.snapshot_id > last_replicated]

for snap in unreplicated:
    snap_id = snap.snapshot_id
    operation = snap.operation

    # Step 2: Get changed files for this snapshot
    added_files = spark.sql(f"""
        SELECT file_path, file_size_in_bytes, record_count, partition
        FROM primary.finance.transactions.all_entries
        WHERE snapshot_id = {snap_id} AND status = 1  -- ADDED
    """).collect()

    removed_files = spark.sql(f"""
        SELECT file_path, file_size_in_bytes, record_count, partition
        FROM primary.finance.transactions.all_entries
        WHERE snapshot_id = {snap_id} AND status = 2  -- DELETED
    """).collect()

    # Step 3: Copy new files to standby S3
    for f in added_files:
        src = f.file_path
        dst = src.replace("s3://primary-bucket/", "s3://standby-bucket/")
        # Use boto3 or aws s3 cp for server-side copy
        import subprocess
        subprocess.run(["aws", "s3", "cp", src, dst, "--quiet"])

    # Step 4: Replay via SQL (simplified — for appends)
    if operation == "append":
        # Read the new files from standby and append to standby table
        # (This is a simplified approach; production would use Java API)
        new_data = spark.read.parquet(
            *[f.file_path.replace("s3://primary-bucket/", "s3://standby-bucket/")
              for f in added_files]
        )
        new_data.writeTo("standby.finance.transactions").append()

    save_checkpoint(snap_id)
    print(f"Replicated snapshot {snap_id} ({operation}): "
          f"+{len(added_files)} files, -{len(removed_files)} files")
```

### A13. Snapshot Replay: Monitor replication lag

```python
# Compare primary vs standby snapshot counts and lag
primary_latest = spark.sql("""
    SELECT snapshot_id, committed_at
    FROM primary.finance.transactions.snapshots
    ORDER BY committed_at DESC LIMIT 1
""").collect()[0]

standby_latest = spark.sql("""
    SELECT snapshot_id, committed_at
    FROM standby.finance.transactions.snapshots
    ORDER BY committed_at DESC LIMIT 1
""").collect()[0]

primary_count = spark.sql("SELECT COUNT(*) FROM primary.finance.transactions.snapshots").collect()[0][0]
standby_count = spark.sql("SELECT COUNT(*) FROM standby.finance.transactions.snapshots").collect()[0][0]

lag_seconds = (primary_latest.committed_at - standby_latest.committed_at).total_seconds()
snapshot_lag = primary_count - standby_count

print(f"Primary: snapshot {primary_latest.snapshot_id} at {primary_latest.committed_at}")
print(f"Standby: snapshot {standby_latest.snapshot_id} at {standby_latest.committed_at}")
print(f"Lag: {lag_seconds:.0f} seconds, {snapshot_lag} snapshots behind")

if lag_seconds > 900:  # 15 minutes
    alert(f"Replication lag exceeds RPO: {lag_seconds/60:.1f} minutes")
```
