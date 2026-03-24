# Apache Iceberg Data Lakehouse: System Design Interview Guide

A framework for designing (or interviewing on) an Iceberg-based data lakehouse.
Covers requirements gathering, capacity estimation, and design decisions.

---

## 1. Questions to Ask (Requirements Gathering)

Before drawing any boxes, nail down the constraints. These 7 questions are the **most important**
— they expose the trade-offs that drive every downstream design decision. Ask them first
in the first 5 minutes of the interview. (Full question list in Appendix A.)

| # | Question | Why It Matters | Design Decisions It Drives |
|---|----------|---------------|--------------------------|
| 1 | What's the **daily ingest volume** and **total table size**? | 100 GB/day vs 10 TB/day are completely different systems | File sizing, partition count, compaction frequency, metadata strategy |
| 2 | **Batch** or **streaming** ingestion? Or both? | Streaming → small files problem → compaction required. Batch → simpler. | Commit frequency, compaction cadence, fanout writers, write distribution mode |
| 3 | Are there **updates/deletes**? What **% of data** changes per operation? How **frequent** are writes? | Drives COW vs MOR. The decision depends on TWO axes — see table below. | COW vs MOR, delete file handling, compaction urgency |

**COW vs MOR quick decision guide** (for question #3):

| Scenario | Best Choice | Why |
|---|---|---|
| Few rows change per commit (~1% of data, CDC trickle) | **MOR** | Tiny delete files vs rewriting entire 256 MB data files — huge write savings |
| Bulk rewrite per commit (~50% of data) | **COW** | Rewriting most files anyway — COW avoids delete file accumulation |
| High write frequency (streaming upserts every minute) | **MOR** | COW would rewrite files every minute → massive write amplification |
| Low write frequency (daily batch, 1x/day) | **COW** | One daily rewrite is affordable; clean files = fastest reads all day |
| Read-heavy, write-rare (dashboards 100x/day, updates 1x/day) | **COW** | Optimize for reads — no merge overhead on the hot read path |
| Write-heavy, read-rare (logging, audit trail) | **MOR** or append | Minimize write cost; reads are infrequent so merge overhead is tolerable |
| 4 | What are the **primary query patterns** — which **columns** appear most in WHERE clauses, and what's the **latency SLA**? | Determines partition key AND sort order — wrong choice = full table scan. High-selectivity filter columns become sort key candidates. | Partition design (hidden partitions, bucket), sort order (WRITE ORDERED BY), distribution mode (RANGE vs HASH), z-order for ad-hoc |
| 5 | What's the **data retention** policy? Is **time travel** needed? How far back? | Retention drives storage cost and snapshot expiration policy. Time travel needs determine how many snapshots to keep. | Snapshot expiration interval, `history.expire.max-snapshot-age-ms`, storage tiering, tag-based snapshot pinning |
| 6 | How many **concurrent writers**? What's the **commit frequency**? | Concurrent writers cause optimistic concurrency conflicts during maintenance. High commit frequency → metadata bloat. | Commit retry config, partial-progress compaction, compaction scheduling (avoid active partitions), manifest merge settings |
| 7 | Is the schema **stable or evolving**? Any **DR/multi-region** needs? | Schema evolution affects all downstream consumers; DR adds replication complexity | Schema evolution plan, partition evolution, replication strategy (S3 CRR vs Iceberg API) |

---

## 2. Capacity Estimation

### Step 1: Data Volume & File Count

```
Given:
  daily_ingest        = 500 GB/day
  avg_record_size     = 1 KB
  records_per_day     = 500M
  target_file_size    = 256 MB (Parquet, compressed)
  records_per_file    = ~250K (at 1 KB/record, ~10:1 Parquet compression)

Derived:
  files_per_day       = 500 GB / 256 MB = ~2,000 files/day
  yearly_data         = 500 GB × 365 = ~180 TB/year
  yearly_files        = 2,000 × 365 = ~730,000 files/year
```

### Step 2: Partition Sizing

```
Partitioning goal:
  Each partition should contain 1-10 GB of data
  Each partition should have 4-40 files (at 256 MB target)

Example — partition by day:
  partition_size      = 500 GB / 1 partition/day = 500 GB  ← TOO LARGE
  → Sub-partition by hour: 500 GB / 24 = ~21 GB            ← borderline
  → Use hidden partition: PARTITIONED BY (days(event_time), bucket(16, user_id))
    partition_size    = 500 GB / (1 × 16) = ~31 GB per day-bucket ← still large
    partition_size    = 500 GB / (24 × 16) = ~1.3 GB per hour-bucket ← good

Example — partition by hour:
  partition_size      = 500 GB / 24 = ~21 GB               ← acceptable
  files_per_partition = 21 GB / 256 MB = ~82 files          ← fine

Rule of thumb:
  TOO SMALL:  partition < 100 MB → over-partitioned, too many small files
  SWEET SPOT: partition = 1-10 GB → good file counts, efficient pruning
  TOO LARGE:  partition > 100 GB → insufficient pruning, slow scans
```

### Step 3: Metadata Sizing

```
Metadata hierarchy:
  metadata.json → manifest list → manifest files → data files

Per-file metadata overhead:
  manifest_entry     ≈ 1-2 KB per data file (path + stats + partition info)
  manifest_file      ≈ 8 MB (holds ~4,000-8,000 file entries)

For 730K files/year:
  manifest_files     = 730K / 5K = ~146 manifest files
  manifest_data      = 146 × 8 MB = ~1.2 GB (total manifest data)

Snapshot metadata:
  snapshot_size      ≈ manifest_list + pointers ≈ a few KB per snapshot
  If committing hourly: 24 snapshots/day × 365 = 8,760 snapshots/year
  → Must expire snapshots regularly (keep 3-7 days = 72-168 snapshots)

metadata.json growth:
  Each snapshot adds ~1-2 KB to metadata.json
  Without expiration: 8,760 × 2 KB = ~17 MB/year (manageable)
  But snapshot references prevent old file cleanup → storage bloat
```

### Step 4: Compaction Budget

```
Streaming ingestion scenario:
  commit_interval     = 1 minute
  files_per_commit    = parallelism = 32
  files_per_hour      = 32 × 60 = 1,920 small files
  file_size           = 500 GB / (24 × 60 × 32) = ~0.45 MB  ← TINY!

  → Must compact: rewrite 1,920 files into ~82 files (256 MB each)
  → Compaction frequency: every 1-4 hours
  → Compaction I/O: read 21 GB + write 21 GB = 42 GB per compaction run
  → Daily compaction I/O: 42 GB × 6-24 runs = 252 GB - 1 TB/day

  Compaction strategy:
    binpack   — fast, groups small files, no sort overhead
    sort      — slower, rewrites with global sort, better read performance
    zorder    — slowest, multi-column clustering for ad-hoc query patterns
```

### Quick Estimation Cheat Sheet

```
┌────────────────────────────────────────────────────────────────┐
│                    ICEBERG CAPACITY ESTIMATION                   │
│                                                                  │
│  daily_ingest / target_file_size = files_per_day                │
│  daily_ingest / num_partitions   = partition_size               │
│    → if < 100 MB: over-partitioned                              │
│    → if 1-10 GB: sweet spot                                     │
│    → if > 100 GB: under-partitioned                             │
│                                                                  │
│  TARGET FILE SIZES (Parquet, compressed):                       │
│    Batch tables:     256 MB – 512 MB                            │
│    Streaming tables: 128 MB – 256 MB (post-compaction)          │
│    Minimum viable:   32 MB (below this, metadata overhead hurts)│
│                                                                  │
│  SNAPSHOT MANAGEMENT:                                           │
│    commits/day × retention_days = active_snapshots              │
│    Keep 3-7 days of snapshots (72-168 for hourly commits)       │
│    Expire older → enables orphan file cleanup                   │
│                                                                  │
│  COMPACTION (streaming tables):                                 │
│    small_files/hour = parallelism × commits_per_hour            │
│    compaction_io = partition_ingest_rate × 2 (read + write)     │
│    frequency: every 1-6 hours (balance freshness vs cost)       │
│                                                                  │
│  METADATA:                                                      │
│    manifest_files ≈ total_data_files / 5,000                    │
│    manifest_size ≈ manifest_files × 8 MB                        │
│    metadata.json ≈ snapshots × 2 KB (keep small via expiration) │
└────────────────────────────────────────────────────────────────┘
```

---

## 3. Interview Flow (45 minutes)

```
┌──────────────────────────────────────────────────────────────────────┐
│  45-MINUTE INTERVIEW TIMELINE                                         │
│                                                                       │
│  0:00 ────── PHASE 1: CLARIFY (5 min) ──────                        │
│  Ask requirements questions from Section 1.                           │
│  Nail down: data volume, query patterns, update frequency, latency.  │
│  This sets up EVERY decision that follows.                            │
│                                                                       │
│  0:05 ────── PHASE 2: ESTIMATE (5 min) ──────                       │
│  Back-of-envelope from Section 2.                                     │
│  File count, partition sizing, metadata growth, compaction budget.    │
│  Shows you can reason about scale before touching the whiteboard.     │
│                                                                       │
│  0:10 ────── PHASE 3: ARCHITECTURE + KEY DECISIONS (15 min) ─────── │
│  Draw high-level lakehouse:                                           │
│                                                                       │
│    ┌─────────┐   ┌──────────────────────────────┐   ┌──────────┐   │
│    │ Ingest   │──▶│  Object Store (S3/GCS/ADLS)  │◀──│ Query    │   │
│    │(Spark/   │   │  ┌──────────────────────────┐│   │(Trino/   │   │
│    │ Flink)   │   │  │  Iceberg Tables          ││   │ Spark/   │   │
│    └─────────┘   │  │  ├── metadata/            ││   │ Athena)  │   │
│                   │  │  ├── data/                ││   └──────────┘   │
│                   │  │  └── delete files (MOR)   ││                  │
│    ┌─────────┐   │  └──────────────────────────┘│                   │
│    │ Catalog  │◀──│                              │                   │
│    │(REST/    │   └──────────────────────────────┘                   │
│    │ Glue/    │                                                      │
│    │ Nessie)  │   ┌──────────────────────────────┐                   │
│    └─────────┘   │  Compaction Service            │                  │
│                   │  (scheduled Spark/Flink job)   │                  │
│                   └──────────────────────────────┘                   │
│                                                                       │
│  Then walk through the 7 Key Decisions (Section 4) — spend the      │
│  most time on decisions #1-#3 which have the deepest trade-offs.     │
│                                                                       │
│  0:25 ────── PHASE 4: DATA LIFECYCLE & MAINTENANCE (10 min) ─────   │
│  Cover decisions #5-#7 in depth:                                     │
│    - Compaction strategy (binpack vs sort vs zorder)                 │
│    - Snapshot expiration and orphan file cleanup                     │
│    - Schema evolution and partition evolution                        │
│    - Streaming small files → compaction → query performance cycle    │
│                                                                       │
│  0:35 ────── PHASE 5: PRODUCTION READINESS (10 min) ────            │
│  Show operational maturity:                                           │
│    - Monitoring: file counts, snapshot counts, commit latency,       │
│      query planning time, scan data volume                           │
│    - Multi-engine access patterns (Spark writes, Trino reads)        │
│    - Catalog HA and metadata caching                                 │
│    - Cost optimization (storage tiers, compaction scheduling)        │
│                                                                       │
│  0:45 ────── END ──────                                              │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. The 7 Key Design Decisions

These are the decisions that differentiate a strong candidate. Each should be **justified
by requirements** from Section 1, not stated as dogma.

### Decision 1: Copy-on-Write vs Merge-on-Read

**The single most impactful decision for tables with updates/deletes.** It determines write
latency, read performance, storage amplification, and compaction needs.

```
┌──────────────────────────────────────────────────────────────────────┐
│  COW vs MOR TRADE-OFF                                                 │
│                                                                       │
│  COPY-ON-WRITE (COW)                  MERGE-ON-READ (MOR)            │
│  ────────────────────                 ────────────────────            │
│  On update/delete: rewrites entire    On update/delete: writes only  │
│  affected data files with changes     a small delete file            │
│  applied                              (position or equality deletes) │
│                                                                       │
│  Write cost: HIGH (rewrite files)     Write cost: LOW (small file)   │
│  Read cost:  LOW (no merge needed)    Read cost: HIGHER (must merge  │
│  Storage:    MODERATE (new files)              delete files at read)  │
│                                       Storage: LOW (until compaction)│
│                                                                       │
│  DELETE FILE TYPES (MOR only):                                       │
│  ─────────────────────────────                                       │
│  Position deletes: tracks (file, row_position) of deleted rows       │
│    → Write: reads data files to find positions (slower write)        │
│    → Read: efficient merge (fast read)                               │
│                                                                       │
│  Equality deletes: tracks column values of deleted rows              │
│    → Write: no data file read needed (fastest write)                 │
│    → Read: must match values against ALL scanned rows (slow read)    │
│                                                                       │
│  DECISION MATRIX:                                                    │
│  ────────────────                                                    │
│  Append-only (no updates)?        → Neither — just append           │
│  Rare updates (<1% of data)?      → MOR with position deletes       │
│  Frequent updates (>10% of data)? → COW (amortize rewrite cost)     │
│  Low write latency critical?      → MOR with equality deletes       │
│  Low read latency critical?       → COW                             │
│  Streaming upserts?               → MOR (avoid rewrite per commit)  │
│                                                                       │
│  KEY INSIGHT: MOR accumulates delete files that degrade reads        │
│  over time. You MUST compact regularly to merge delete files back    │
│  into data files. Without compaction, MOR read performance decays.   │
└──────────────────────────────────────────────────────────────────────┘
```

**How to answer**: "Given the update frequency of X% and the read latency SLA of Y, I would
choose Z because..."

Ref: `write-ordered-by-vs-distribution-mode.md` (for write distribution during COW/MOR operations)

### Decision 2: Partitioning Strategy

**Partitioning determines query pruning effectiveness.** Bad partitioning = full table scans
on every query. Over-partitioning = thousands of tiny files.

```
┌──────────────────────────────────────────────────────────────────────┐
│  PARTITIONING STRATEGY                                                │
│                                                                       │
│  IDENTITY vs HIDDEN (TRANSFORM) PARTITIONS:                          │
│  ──────────────────────────────────────────                          │
│  Identity:  PARTITIONED BY (date)                                    │
│    → partition column exists in schema                               │
│    → users must know partition structure                             │
│                                                                       │
│  Hidden:    PARTITIONED BY (days(event_time), bucket(16, user_id))   │
│    → partition column computed from source column via transform      │
│    → users query source columns; Iceberg prunes automatically       │
│    → transforms: identity, bucket, truncate, year, month, day, hour │
│                                                                       │
│  PARTITION EVOLUTION (unique to Iceberg):                            │
│  ────────────────────────────────────────                            │
│  Can change partition strategy without rewriting data!               │
│    ALTER TABLE t ADD PARTITION FIELD hours(event_time)               │
│  Old files keep old partition spec; new files use new spec.          │
│  Iceberg handles mixed specs transparently at query time.            │
│                                                                       │
│  SIZING RULES:                                                       │
│  ─────────────                                                       │
│  Target: 1-10 GB per partition                                       │
│  Too small (<100 MB): over-partitioned → metadata overhead           │
│  Too large (>100 GB): under-partitioned → poor query pruning         │
│                                                                       │
│  COMMON PATTERNS:                                                    │
│  ────────────────                                                    │
│  Time-series data:  PARTITIONED BY (days(event_time))                │
│  High-volume time:  PARTITIONED BY (hours(event_time))               │
│  Multi-tenant:      PARTITIONED BY (tenant_id, days(event_time))     │
│  User activity:     PARTITIONED BY (days(event_time),                │
│                                     bucket(32, user_id))             │
│  No obvious key:    Don't partition — use sort order instead         │
│                                                                       │
│  ANTI-PATTERNS:                                                      │
│  ──────────────                                                      │
│  ✗ Partitioning by high-cardinality column (user_id alone)           │
│  ✗ More than 2-3 levels of partitioning                              │
│  ✗ Partitioning by a column rarely used in WHERE clauses             │
│  ✗ Ignoring partition evolution when data patterns change            │
└──────────────────────────────────────────────────────────────────────┘
```

### Decision 3: Sort Order & Write Distribution Mode

**Sort order determines file-level min/max statistics**, which enable Spark/Trino to skip
files that don't contain relevant data. This is the most underrated performance lever.

```
┌──────────────────────────────────────────────────────────────────────┐
│  SORT ORDER & DISTRIBUTION MODE                                       │
│                                                                       │
│  WHY SORT ORDER MATTERS:                                             │
│  ───────────────────────                                             │
│  Without sort order: each file contains random value ranges          │
│    → min/max stats are useless → no file pruning → full scan        │
│                                                                       │
│  With sort order (e.g., WRITE ORDERED BY user_id):                   │
│    → each file contains a tight range of user_ids                    │
│    → WHERE user_id = 123 skips 90%+ of files via min/max stats      │
│                                                                       │
│  DISTRIBUTION MODE (how Spark shuffles before writing):              │
│  ──────────────────────────────────────────────────                  │
│  RANGE (default for sorted tables):                                  │
│    Global range sort → files have non-overlapping value ranges       │
│    Best for: range queries, point lookups on sort key                │
│    Cost: expensive shuffle (full range partition)                    │
│                                                                       │
│  HASH (default for unsorted partitioned tables):                     │
│    Hash by partition columns → data clustered by partition           │
│    Sort order applied locally within each partition only             │
│    Best for: partition-scoped queries                                │
│    Cost: cheaper shuffle                                             │
│                                                                       │
│  NONE:                                                               │
│    No shuffle → local sort within each Spark task only               │
│    Best for: append-only, no query pattern to optimize for           │
│    Cost: minimal write overhead                                      │
│                                                                       │
│  DDL SYNTAX → MODE MAPPING:                                         │
│  ──────────────────────────                                          │
│  WRITE ORDERED BY col                → mode=RANGE                   │
│  WRITE DISTRIBUTED BY PARTITION      → mode=HASH                    │
│  WRITE DISTRIBUTED BY PARTITION                                      │
│    ORDERED BY col                    → mode=HASH (local sort)       │
│  WRITE LOCALLY ORDERED BY col        → mode unchanged (local sort)  │
│                                                                       │
│  DECISION GUIDE:                                                     │
│  ───────────────                                                     │
│  Point lookups on specific column?  → WRITE ORDERED BY that column  │
│  Range scans on time + dimension?   → WRITE ORDERED BY time, dim    │
│  Ad-hoc queries on multiple cols?   → Z-ORDER during compaction     │
│  Partition-scoped queries only?     → DISTRIBUTED BY PARTITION      │
│  Append-only, read-all workload?    → No sort order needed          │
└──────────────────────────────────────────────────────────────────────┘
```

Ref: `write-ordered-by-vs-distribution-mode.md` (detailed code-level analysis)

### Decision 4: File Sizing & Compaction Strategy

**File sizing directly impacts query planning time and scan efficiency.** Too many small files
= slow planning. Too few large files = poor pruning granularity.

```
┌──────────────────────────────────────────────────────────────────────┐
│  FILE SIZING & COMPACTION                                             │
│                                                                       │
│  TARGET FILE SIZES (Parquet, compressed):                            │
│  ────────────────────────────────────────                            │
│  < 32 MB:    Too small — metadata overhead dominates                 │
│  32-128 MB:  Acceptable for streaming pre-compaction                 │
│  128-512 MB: Sweet spot for most workloads                           │
│  > 1 GB:     Too large — reduces pruning granularity                 │
│                                                                       │
│  Recommendation: 256 MB target (default in most engines)             │
│                                                                       │
│  COMPACTION STRATEGIES:                                              │
│  ─────────────────────                                               │
│  binpack:                                                            │
│    Combines small files into target-sized files                      │
│    No sorting — preserves existing order                             │
│    Fastest, lowest I/O cost                                          │
│    Best for: streaming tables needing quick cleanup                  │
│                                                                       │
│  sort:                                                               │
│    Rewrites files with global sort on specified columns              │
│    Produces files with tight, non-overlapping value ranges           │
│    Higher I/O cost (reads + sorts + writes all data)                 │
│    Best for: tables with known query patterns on sort key            │
│                                                                       │
│  zorder:                                                             │
│    Multi-dimensional clustering (interleaves sort on N columns)      │
│    Good for ad-hoc queries on any combination of those columns       │
│    Highest I/O cost                                                  │
│    Best for: tables queried by multiple dimensions (no single key)   │
│                                                                       │
│  COMPACTION CADENCE:                                                 │
│  ──────────────────                                                  │
│  Streaming tables:  every 1-6 hours (balance freshness vs cost)      │
│  Batch tables:      after each large batch job, or daily             │
│  MOR tables:        also rewrite delete files (rewritePositionDeletes│
│                     or full rewriteDataFiles with deletes applied)   │
│                                                                       │
│  MONITORING:                                                         │
│  ──────────                                                          │
│  Track: files_per_partition, avg_file_size, delete_file_count        │
│  Alert: if files_per_partition > 500 or avg_file_size < 32 MB       │
└──────────────────────────────────────────────────────────────────────┘
```

### Decision 5: Catalog Choice

**The catalog is the single point of coordination** for all engines accessing the table.
Wrong choice = multi-engine incompatibility, no branching, or vendor lock-in.

```
┌──────────────────────────────────────────────────────────────────────┐
│  CATALOG COMPARISON                                                   │
│                                                                       │
│  Catalog        Multi-Engine  Branching  Managed  Notes              │
│  ──────────     ────────────  ─────────  ───────  ─────              │
│  REST Catalog   YES (best)    depends*   no       Vendor-neutral API │
│  AWS Glue       YES (AWS)     no         yes      AWS-native, free   │
│  Hive Metastore YES           no         no       Legacy, widespread │
│  Nessie         YES (REST)    YES        no       Git-like versioning│
│  Polaris        YES (REST)    YES        yes      Snowflake-backed   │
│                                                                       │
│  * REST Catalog is a spec; branching depends on the implementation   │
│                                                                       │
│  DECISION GUIDE:                                                     │
│  ───────────────                                                     │
│  AWS-only, simple setup?           → Glue Catalog                   │
│  Multi-cloud, multi-engine?        → REST Catalog                   │
│  Need Git-like branching/tagging?  → Nessie                         │
│  Already have Hive ecosystem?      → Hive Metastore (migrate later) │
│  Enterprise, managed?              → Polaris or Tabular             │
│                                                                       │
│  KEY CONSIDERATIONS:                                                 │
│  ───────────────────                                                 │
│  - Catalog must support atomic metadata pointer swap (commit)       │
│  - Catalog is on the critical path for EVERY read and write         │
│  - Catalog HA is essential — if catalog is down, nothing works      │
│  - Metadata caching at query engine level reduces catalog load      │
│  - REST Catalog is the emerging standard — prefer it for new builds │
└──────────────────────────────────────────────────────────────────────┘
```

### Decision 6: Schema Evolution & Table Maintenance

```
┌──────────────────────────────────────────────────────────────────────┐
│  SCHEMA EVOLUTION & MAINTENANCE                                       │
│                                                                       │
│  SCHEMA EVOLUTION (Iceberg's strength):                              │
│  ──────────────────────────────────────                              │
│  Iceberg tracks schema by field ID, not by name or position.         │
│  This enables safe, backward-compatible changes:                     │
│                                                                       │
│  Safe operations (no rewrite needed):                                │
│    ADD column          → old files return null for new column        │
│    DROP column         → old files still readable, column ignored    │
│    RENAME column       → tracked by ID, not name                     │
│    REORDER columns     → tracked by ID, not position                 │
│    WIDEN type          → int → long, float → double                  │
│    Make nullable       → required → optional                         │
│                                                                       │
│  Unsafe (NOT supported):                                             │
│    NARROW type         → long → int (data loss risk)                 │
│    Change type family  → string → int                                │
│                                                                       │
│  TABLE MAINTENANCE CHECKLIST:                                        │
│  ────────────────────────────                                        │
│  1. Snapshot expiration                                              │
│     CALL catalog.system.expire_snapshots('t', older_than => ...)    │
│     Retain 3-7 days; enables old data file cleanup                  │
│                                                                       │
│  2. Orphan file cleanup                                              │
│     CALL catalog.system.remove_orphan_files('t')                    │
│     Removes data files not referenced by any snapshot                │
│     Run AFTER snapshot expiration                                    │
│                                                                       │
│  3. Rewrite data files (compaction)                                  │
│     CALL catalog.system.rewrite_data_files('t')                     │
│     Combines small files, optionally sorts                          │
│                                                                       │
│  4. Rewrite manifests                                                │
│     CALL catalog.system.rewrite_manifests('t')                      │
│     Optimizes manifest file sizes for better query planning          │
│                                                                       │
│  MAINTENANCE SCHEDULE (production):                                  │
│  ──────────────────────────────────                                  │
│  Hourly:  compact streaming partitions (binpack or sort)            │
│  Daily:   expire snapshots older than 7 days                        │
│  Weekly:  remove orphan files, rewrite manifests                    │
│  Monthly: full sort compaction on historical partitions             │
│                                                                       │
│  EXECUTION ORDER (when running multiple):                            │
│    1. rewrite_data_files — creates new files + snapshots             │
│    2. expire_snapshots — removes old snapshot references             │
│    3. remove_orphan_files — cleans up unreferenced files             │
│    4. rewrite_manifests — optimizes metadata layer                   │
│                                                                       │
│  CONCURRENT WRITE CONFLICTS:                                         │
│  ───────────────────────────                                         │
│  Maintenance jobs can fail with CommitFailedException when           │
│  concurrent writers commit during compaction. Key mitigations:       │
│    1. Compact OLD partitions only (never the active write partition) │
│    2. Enable partial-progress for incremental commits               │
│    3. Use binpack (not sort) for streaming tables                   │
│    4. Increase commit.retry.num-retries for busy tables             │
│  See: iceberg-table-monitoring-and-maintenance-guide.md Section 9   │
└──────────────────────────────────────────────────────────────────────┘
```

### Decision 7: Streaming vs Batch Ingestion

```
┌──────────────────────────────────────────────────────────────────────┐
│  STREAMING vs BATCH INGESTION                                         │
│                                                                       │
│  BATCH (Spark batch, hourly/daily jobs):                             │
│  ───────────────────────────────────────                             │
│  + Naturally produces well-sized files (256 MB+)                    │
│  + WRITE ORDERED BY works well (full dataset available for sort)    │
│  + Simple: run job, commit, done                                    │
│  - Higher latency (minutes to hours)                                │
│  - Data not available until job completes                           │
│                                                                       │
│  STREAMING (Flink or Spark Structured Streaming):                    │
│  ────────────────────────────────────────────────                    │
│  + Low latency (seconds to minutes)                                 │
│  + Continuous, near-real-time data availability                     │
│  - Small files problem: each commit produces many tiny files        │
│  - Must run compaction service alongside                            │
│  - Higher operational complexity                                    │
│                                                                       │
│  THE SMALL FILES PROBLEM:                                            │
│  ────────────────────────                                            │
│  Streaming commit every 1 min with 32 writers:                      │
│    → 32 files × 60 commits/hr = 1,920 small files/hour             │
│    → each file may be < 1 MB                                        │
│    → query planning slows as file count grows                       │
│    → min/max stats span wide ranges → no pruning benefit            │
│                                                                       │
│  SOLUTIONS:                                                          │
│  ──────────                                                          │
│  1. Increase commit interval (trade latency for file size)          │
│     → 5-10 min intervals → fewer, larger files                     │
│                                                                       │
│  2. Use write.distribution-mode=hash                                │
│     → cluster by partition → fewer files per partition per commit   │
│                                                                       │
│  3. Fanout writers (Iceberg default in Spark)                       │
│     → each task writes to multiple partitions simultaneously        │
│     → reduces file count when data spans many partitions            │
│                                                                       │
│  4. Scheduled compaction (THE essential solution)                    │
│     → binpack small files into 256 MB targets every 1-4 hours      │
│     → optionally sort for better query performance                  │
│                                                                       │
│  5. Hybrid: stream to "hot" partition, batch-compact to "warm"      │
│     → recent data: small files, fast ingest                         │
│     → older data: compacted, sorted, optimized for reads            │
│                                                                       │
│  FLINK vs SPARK STREAMING:                                           │
│  ─────────────────────────                                           │
│  Flink:  true streaming, per-checkpoint commits (~seconds)          │
│          lower latency, but more small files                        │
│          built-in Iceberg sink with exactly-once via checkpoint     │
│                                                                       │
│  Spark:  micro-batch, per-batch commits (~seconds to minutes)       │
│          slightly higher latency, slightly larger files             │
│          Structured Streaming with foreachBatch or Iceberg sink     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 5. Appendix: All Design Decisions Ranked

Complete list from most to least important. The top 7 (marked with **) are covered in
depth in Section 4. The rest are follow-up discussion topics if time permits or if the
interviewer drills into a specific area.

| Rank | Decision | One-Line Justification |
|------|----------|----------------------|
| **1** | **COW vs MOR** | Drives write latency, read cost, storage amplification, and compaction needs |
| **2** | **Partitioning strategy** | Determines query pruning — wrong partition = full table scan every time |
| **3** | **Sort order & distribution mode** | Controls file-level min/max stats — the most underrated read performance lever |
| **4** | **File sizing & compaction** | Too many small files = slow planning; too few large files = poor pruning |
| **5** | **Catalog choice** | Single point of coordination for all engines; affects multi-engine, branching, HA |
| **6** | **Schema evolution & maintenance** | Snapshot expiration, orphan cleanup, manifest rewriting prevent metadata bloat |
| **7** | **Streaming vs batch ingestion** | Determines small files problem severity and compaction requirements |
| 8 | Snapshot retention policy | Too short = no time travel; too long = metadata bloat + storage cost |
| 9 | Object store choice & layout | S3 vs GCS vs ADLS; prefix design affects listing performance and throttling |
| 10 | Data format (Parquet vs ORC vs Avro) | Parquet dominant; ORC for Hive ecosystem; Avro for streaming append |
| 11 | Compression codec | Zstd (best ratio), Snappy (fastest), LZ4 (balanced) — affects file size and CPU |
| 12 | Column statistics & bloom filters | write.metadata.metrics.default, bloom filters for high-cardinality lookups |
| 13 | Partition evolution strategy | When and how to evolve partitions as data patterns change |
| 14 | Multi-engine access patterns | Spark writes + Trino reads — catalog compatibility, metadata caching |
| 15 | Concurrent writer handling | Optimistic concurrency, retry logic, conflict resolution. See monitoring guide Section 9. |
| 16 | Time travel & audit requirements | Snapshot retention for regulatory compliance or debugging |
| 17 | Branching & tagging (Nessie) | Isolate experimental writes, A/B testing, staging validation. See ML feature store doc. |
| 18 | Multi-region DR & replication | S3 CRR vs Iceberg API (RewriteTablePath) vs Snapshot Replay. See multi-region DR doc. |
| 19 | Migration from Hive/Parquet | 4-phase shadow migration: snapshot_table → dual-write → cutover → decommission. See migration doc. |
| 20 | Cost optimization | Storage tiers (S3 IA for old data), compaction scheduling in off-peak hours |
| 21 | Monitoring & alerting | 12 key metrics, health scoring, automated maintenance triggers. See monitoring guide. |
| 22 | CDC & changelog consumption | `.changes` table, `create_changelog_view`, carry-over removal. See changelog deep dive. |
| 23 | Security & access control | Fine-grained column/row-level security, encryption at rest |

---

## Appendix A: Full Requirements Gathering Questions

The 7 key questions in Section 1 cover the interview. Below is the complete list organized
by category, useful as a reference checklist or for deeper follow-up discussions.

### Data Characteristics

| Question | Why it matters |
|----------|---------------|
| What's the **data format** today? (CSV, JSON, Avro, Parquet, ORC?) | Determines migration cost and whether you can adopt Iceberg incrementally |
| What's the **record size**? Avg and P99? | Drives file sizing targets. 100-byte records need more rows per file than 10 KB records. |
| What's the **schema complexity**? Nested structs? Maps? | Deep nesting increases Parquet column count, affecting metadata size and column pruning |
| Is the schema **stable or evolving**? How often? | Frequent evolution → need Iceberg's schema evolution; must plan for add/rename/drop/reorder |
| What's the **natural key**? Is there a primary key? | Determines dedup strategy, merge logic, and whether equality deletes are viable |
| What's the **time dimension**? Event time? Arrival time? | Drives partitioning strategy (usually partition by time) and incremental read patterns |

### Volume & Growth

| Question | Why it matters |
|----------|---------------|
| What's the **daily ingest volume**? (GB/day or TB/day?) | Drives file count, partition sizing, compaction frequency |
| What's the **total table size**? (TB or PB?) | Determines metadata management strategy — a 100 TB table has very different metadata needs than 1 TB |
| How many **records/day** are ingested? | Combined with record size, determines target file count per partition |
| What's the **data retention**? (days, months, forever?) | Drives snapshot expiration, partition-level deletes, storage cost |
| What's the **growth rate**? (e.g., 2x/year?) | Affects partition strategy — high-cardinality partitions that work today may not at 10x scale |

### Query Patterns

| Question | Why it matters |
|----------|---------------|
| What are the **primary query patterns**? (point lookup? range scan? full scan? aggregation?) | The single most impactful driver for partitioning and sort order |
| What **columns** appear most in WHERE clauses? | Partition and sort order candidates. High selectivity = good for sort order. |
| What's the **query latency SLA**? (sub-second? seconds? minutes?) | Sub-second → need aggressive file pruning, small file counts, possibly materialized views |
| How many **concurrent queries**? | Affects catalog load, metadata caching, and object store request rates |
| Is there **time travel** usage? How far back? | Determines snapshot retention policy — more snapshots = more metadata |
| Are there **incremental reads**? (CDC consumers downstream?) | Determines snapshot commit frequency and whether streaming reads are needed |

### Write Patterns

| Question | Why it matters |
|----------|---------------|
| **Batch** or **streaming** ingestion? Or both? | Streaming → small files problem → need compaction. Batch → larger files naturally. |
| What's the **commit frequency**? (per minute? per hour?) | Each commit = new snapshot + manifests. High frequency → metadata growth → need expiration. |
| Are there **updates/deletes**? How frequent? What percentage of data? | Drives COW vs MOR decision. 1% updates → MOR. 50% rewrites → COW. |
| Is it **append-only** or **upsert**? | Append-only is simplest. Upsert needs merge strategy (COW or MOR). |
| How many **writers**? Concurrent? | Concurrent writers → optimistic concurrency conflicts → need retry logic and compaction conflict avoidance |
| What **engine** writes? (Spark batch? Spark Structured Streaming? Flink?) | Engine determines available write modes, distribution options, commit mechanisms |

### Operations & Infrastructure

| Question | Why it matters |
|----------|---------------|
| What **object store**? (S3, GCS, ADLS, HDFS?) | Determines consistency model, rename atomicity, listing performance |
| What **catalog**? (REST, Glue, Hive Metastore, Nessie?) | Determines multi-engine access, branching support, transaction isolation |
| What **compute engines** read the data? (Spark, Trino, Dremio, Athena, Flink?) | Multi-engine → need catalog compatibility and consistent metadata access |
| Is there a **compaction** process? Automated or manual? | Without compaction, streaming tables degrade over time. Concurrent write conflicts must be managed. |
| What's the **team's operational maturity**? | Determines whether self-managed (open-source) or managed service (AWS, Tabular, Dremio) |
| Is **multi-region DR** required? What's the RPO/RTO? | Adds replication complexity — S3 CRR vs Iceberg API vs snapshot replay |
| Is there a **migration** from Hive/Parquet? | Shadow migration (4-phase) with `snapshot_table`, `migrate`, `add_files` APIs |

---

## 6. Related Documents in This Repo

### Deep Dives (Code-Level Analysis)
- `write-ordered-by-vs-distribution-mode.md` — WRITE ORDERED BY vs distribution mode conflict,
  fanout writers, hidden partition transforms, sort order prepending, and `defaultWriteDistributionMode()`
- `changelog-view-and-mor.md` — Changelog view architecture and MOR limitations
- `iceberg-changelog-view-deep-dive.md` — How `.changes` table and `create_changelog_view` work,
  carry-over removal, compute_updates, net_changes, with concrete data examples

### System Design Problems (Interview Practice)
- `iceberg-design-realtime-ecommerce-analytics.md` — Kafka → Flink → Iceberg → Trino dashboards,
  streaming small files, compaction strategy, schema/partition evolution
- `iceberg-design-multi-region-disaster-recovery.md` — 3 replication alternatives (S3 CRR,
  RewriteTablePath, Snapshot Replay), atomic consistency, catalog sync, RPO/RTO
- `iceberg-design-cdc-customer360-scd2.md` — CDC with SCD Type-2, MERGE INTO, temporal joins,
  surrogate keys, MOR vs COW for dimension tables
- `iceberg-design-ml-feature-store.md` — Schema enforcement, time travel, tags, branches for
  ML reproducibility and experimentation
- `iceberg-design-hive-to-iceberg-shadow-migration.md` — 4-phase shadow migration
  (snapshot_table, dual-write, read cutover, decommission), shared files problem

### Operations Guides
- `iceberg-table-monitoring-and-maintenance-guide.md` — 12 key metrics with thresholds,
  SQL monitoring queries, health scoring, maintenance decision matrix, concurrent conflict
  avoidance (8 best practices), and Airflow orchestration examples
