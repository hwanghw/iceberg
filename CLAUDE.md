# Apache Iceberg - Developer Guide

## Project Overview

Apache Iceberg is an open table format for huge analytic datasets. It brings
SQL-table reliability (ACID transactions, schema evolution, hidden
partitioning, time travel, and snapshot isolation) to data lakes, and lets
multiple compute engines — Spark, Flink, Hive, Trino, and others — safely
operate on the same tables concurrently.

The project is split into a small, engine-independent **core** (the table
format, metadata, and APIs) and a wide set of **integration modules** that plug
that core into specific storage systems, catalogs, file formats, and query
engines. This repository is the reference **Java** implementation.

## Essential Build & Development Commands

Iceberg is built with **Gradle** and requires **Java 17 or 21**.

```bash
# Full build with tests
./gradlew build

# Build, skipping tests (fast)
./gradlew build -x test -x integrationTest

# Build a single module
./gradlew :iceberg-core:build

# Run tests for a single module
./gradlew :iceberg-core:test

# Run a single test class
./gradlew :iceberg-core:test --tests org.apache.iceberg.TestTables

# Apply code style (Spotless + Palantir baseline) — run before committing
./gradlew spotlessApply

# Apply code style across all Spark/Hive/Flink versions
./gradlew spotlessApply -DallModules
```

### Engine version matrix

The Spark and Flink integrations are built per engine version (Spark also per
Scala version). Defaults and known versions live in `gradle.properties`
(Flink default `2.1`, known `1.20,2.0,2.1`; Spark default `4.1`, known
`3.4,3.5,4.0,4.1`; Scala default `2.12`, known `2.12,2.13`; Kafka `3`). Select
versions with system properties:

```bash
# Build Iceberg's Spark 3.5 (Scala 2.12) integration
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 \
  :iceberg-spark:iceberg-spark-3.5_2.12:build

# Build Iceberg's Flink 1.20 integration
./gradlew -DflinkVersions=1.20 :iceberg-flink:iceberg-flink-1.20:build
```

> Note: integration tests require Docker (Testcontainers). See `README.md` for
> macOS Docker socket / SELinux workarounds.

## High-Level Architecture

Iceberg is layered: a stable public API, a core implementation of the table
format, pluggable storage (`FileIO`) and `Catalog` backends, file-format
adapters, and engine integrations on top.

### Layered design

```
┌──────────────────────────────────────────────────────────────┐
│  Query Engines                                                 │
│   Spark │ Flink │ Hive/MR │ Kafka Connect │ (Trino, etc.)      │
├──────────────────────────────────────────────────────────────┤
│  Catalogs                          │  File Formats             │
│   Hive │ REST │ JDBC │ Nessie │     │   Parquet │ ORC │ Avro    │
│   Glue │ Snowflake │ BigQuery │…    │   Arrow (vectorized)     │
├──────────────────────────────────────────────────────────────┤
│  iceberg-core   (table format implementation)                 │
│   TableMetadata │ Snapshots │ Manifests │ Catalog/Table ops    │
├──────────────────────────────────────────────────────────────┤
│  iceberg-api    (public interfaces: Table, Schema, Scan, …)    │
├──────────────────────────────────────────────────────────────┤
│  Storage:  FileIO  (S3 │ ADLS │ GCS │ OSS │ ECS │ HDFS │ local)│
└──────────────────────────────────────────────────────────────┘
```

### Module map

**Foundation**
```
api/             # Public, stable interfaces (Table, Schema, Snapshot, Scan, Catalog, expressions, types)
core/            # Core implementation: table metadata, manifests, snapshots, catalogs, REST/JDBC/Hadoop
common/          # Tiny reflection utilities (DynClasses/DynMethods/DynFields/DynConstructors)
data/            # Engine-independent generic record readers/writers
bundled-guava/   # Shaded+relocated Guava consumed by other modules
```

**File formats**
```
parquet/    # Apache Parquet read/write mapped to Iceberg schemas
orc/        # Apache ORC read/write
arrow/      # Apache Arrow vectorized reads
```

**Storage (FileIO) & cloud**
```
aws/        # S3FileIO, GlueCatalog, DynamoDB, LakeFormation, S3 signer
azure/      # ADLS Gen2 FileIO
gcp/        # GCS FileIO
aliyun/     # Alibaba Cloud OSS FileIO
dell/       # Dell EMC ECS object storage (FileIO + catalog)
*-bundle/   # Shaded fat-jars (aws-bundle, azure-bundle, gcp-bundle) for engine runtimes
```

**Catalogs / metastores**
```
hive-metastore/  # HiveCatalog (Hive Metastore as Iceberg catalog)
nessie/          # Project Nessie (Git-like versioned catalog)
snowflake/       # Snowflake-managed Iceberg tables (read-only)
bigquery/        # BigQuery Metastore catalog (org.apache.iceberg.gcp.bigquery)
open-api/        # Iceberg REST Catalog OpenAPI spec + compatibility-kit test server
```

**Engine integrations**
```
spark/      # Spark DataSourceV2 catalog/scan/write + SQL extensions (v3.4, v3.5, v4.0, v4.1)
flink/      # Flink Table API catalog + FLIP-27 source + sink (v1.20, v2.0, v2.1)
mr/         # Hadoop MapReduce InputFormat (read integration; mapreduce + legacy mapred)
kafka-connect/   # Kafka Connect sink with control-topic commit coordination
delta-lake/      # Delta Lake → Iceberg migration/interop
```

**Build-only Gradle project**
```
bom/             # iceberg-bom — published Bill of Materials pinning all module versions
```

**Non-module directories (no `CLAUDE.md`)**
```
format/     # Authoritative on-disk format specs (spec.md, view-spec.md, puffin-spec.md, ...)
docs/, site/# Documentation sources + the iceberg.apache.org site (mkdocs)
examples/   # Example/sample code
dev/, docker/, project/, .baseline/   # Tooling, CI, Docker, and Palantir baseline config
```

### Key architectural concepts

**Table format & metadata** (`iceberg-core`)
- A table is described by an immutable **table metadata** JSON file; each commit
  writes a new metadata file and atomically swaps the catalog's pointer to it.
- A **snapshot** captures the table state at a point in time; snapshots enable
  time travel and snapshot isolation.
- Snapshots reference a **manifest list**, which references **manifest files**,
  which list the actual **data files** and **delete files** with column-level
  stats used for scan planning and predicate pushdown.

**Schema, partitioning, expressions** (`iceberg-api`)
- Rich type system (`Types`), schema evolution by field ID (not by name/position).
- **Hidden partitioning** via partition transforms (`bucket`, `truncate`,
  `year/month/day/hour`, `identity`) — queries don't need partition predicates.
- A unified `Expression` tree drives partition pruning and file filtering.

**Pluggable storage & catalogs**
- `FileIO` (api/core) abstracts file access; implementations live in the cloud
  modules (S3, ADLS, GCS, OSS, ECS) plus Hadoop/local/in-memory in core.
- `Catalog` + `TableOperations` abstract metadata storage and the atomic commit;
  implementations include Hive, REST, JDBC, Glue, Nessie, Snowflake, BigQuery,
  Hadoop.

**Row-level deletes**
- Position and equality delete files support MERGE/UPDATE/DELETE; engines apply
  deletes at read time (merge-on-read) or rewrite data (copy-on-write).

### Dependency direction

A module only depends on modules **below** it. `bundled-guava` is consumed
(shaded) by almost everything and is omitted from the arrows for clarity.

```
engines:   spark        flink        mr            kafka-connect
             │            │           │                  │
   ┌─────────┘            │           ├─ hive-metastore  ├─ kafka-connect-events
   ▼                      ▼           ▼                  ▼
 arrow ─────────────► parquet, orc   data ────────────► (api)
   │                      │           │
   └──────────────► data ─┘           │
                          │           │
   catalogs:  hive-metastore  nessie  snowflake  bigquery     (each → core → api)
   cloud:     aws  azure  gcp  aliyun  dell                   (each → core → api)
                          │
                          ▼
                        core ───────────► api
                          │                 │
                          └──► common ◄─────┘
                                 │
                                 ▼
                           bundled-guava   (relocated Guava)

bundles (shaded fat-jars of the *cloud SDKs only*, NOT the iceberg cloud code;
runtime companions to the cloud modules, which declare those SDKs compileOnly):
   aws-bundle   → relocated AWS SDK v2      (companion to aws)
   azure-bundle → relocated Azure SDK       (companion to azure)
   gcp-bundle   → relocated GCP libraries   (companion to gcp / bigquery)
```

Notes on edges that often surprise:
- `data` depends on `parquet`/`orc` only `compileOnly` (provide them at runtime).
- `arrow` depends on `parquet` (its vectorized reads build on it).
- `mr` and `flink` depend on `hive-metastore` at compile time (for `HiveCatalog`);
  in `spark` the `spark` subproject treats `hive-metastore` as a **test-only**
  dep, and it is the `spark-runtime` jar that bundles it. `spark` also depends on
  `arrow`.
- The cloud `*-bundle` modules bundle only the **cloud SDK** (relocated), *not*
  the Iceberg cloud code — they are runtime companions to `aws`/`azure`/`gcp`,
  which declare those SDKs `compileOnly`.
- The deployable **runtime jars** bundle the Iceberg cloud + Hive catalog
  modules directly: `spark-runtime` shades `aws`, `azure`, `gcp`, `bigquery`,
  `hive-metastore`; `flink-runtime` shades `aws`, `azure`, `gcp`, `bigquery`.
- `bigquery` shares the `org.apache.iceberg.gcp` package with `gcp` but does
  **not** depend on the `gcp` module.

### Per-module dependency table

| Module | Depends on (Iceberg modules) | Notable external libs |
|---|---|---|
| `bundled-guava` | — | Guava (shaded) |
| `api` | bundled-guava | (annotations only) |
| `common` | bundled-guava | — |
| `core` | api, common, bundled-guava | Jackson, Caffeine, Avro, HttpClient5, RoaringBitmap |
| `data` | api, core; *(parquet, orc — compileOnly)* | Avro (compileOnly) |
| `parquet` | api, core, common, bundled-guava | Apache Parquet |
| `orc` | api, core, common, bundled-guava | Apache ORC |
| `arrow` | api, core, parquet, bundled-guava | Apache Arrow, Netty |
| `aws` | api, core, common, bundled-guava | AWS SDK v2 |
| `azure` | api, core, common, bundled-guava | Azure SDK |
| `gcp` | api, core, common, bundled-guava | Google Cloud libs |
| `aliyun` | api, core, common, bundled-guava | Aliyun OSS SDK |
| `dell` | core, common, bundled-guava | Dell ECS client |
| `hive-metastore` | api, core, common, bundled-guava | Hive metastore client |
| `nessie` | api, core, common, bundled-guava | Nessie client |
| `snowflake` | core, common, bundled-guava | Snowflake JDBC |
| `bigquery` | api, core, common, bundled-guava | BigQuery client |
| `open-api` | *(test only)* api, core, aws/gcp/azure/bigquery, bundles | OpenAPI tooling |
| `aws-bundle` | — *(bundles the SDK, not iceberg-aws)* | AWS SDK v2 (shaded) |
| `azure-bundle` | — *(bundles the SDK, not iceberg-azure)* | Azure SDK (shaded) |
| `gcp-bundle` | — *(bundles the libs, not iceberg-gcp)* | Google Cloud libs (shaded) |
| `mr` | api, core, common, data, hive-metastore, orc, parquet | Hadoop 3, Caffeine |
| `spark` (per ver.) | api, core, common, data, orc, parquet, arrow *(+hive-metastore test-only; runtime jar adds aws/azure/gcp/bigquery/hive-metastore)* | Spark, Parquet, Arrow, ORC |
| `flink` (per ver.) | api, core, common, data, orc, parquet, hive-metastore *(runtime jar adds aws/azure/gcp/bigquery)* | Flink |
| `kafka-connect` | api, core, common, data, kafka-connect-events *(+hive-metastore compileOnly)* | Kafka Connect, Avro |

`iceberg-api` and `iceberg-core` are the foundation everything builds on; build
them first when iterating across modules.

## Development Workflow Notes

- Gradle is the only build tool; there is no Maven build.
- Run `./gradlew spotlessApply` before committing — CI enforces formatting and
  the Palantir baseline (`.baseline/`, `baseline.gradle`).
- Unit tests use JUnit 5 with AssertJ; integration tests run through the
  `integrationTest` task and may need Docker, cloud credentials, or containers.
- Spark and Flink code is maintained **per engine version** under `vX.Y/`
  subdirectories; a change often must be applied to each supported version.
- The REST Catalog protocol is specified in
  `open-api/rest-catalog-open-api.yaml` — treat the spec as the source of truth.
- The format specifications live under `format/` (`spec.md`, `view-spec.md`,
  `puffin-spec.md`, etc.) — the authoritative definition of the on-disk format.

## Module-Specific Guides

Each major module has its own `CLAUDE.md`:

- [api](api/CLAUDE.md) — Public interfaces and type system
- [core](core/CLAUDE.md) — Table format implementation, catalogs, REST/JDBC
- [common](common/CLAUDE.md) — Reflection utilities · [bundled-guava](bundled-guava/CLAUDE.md) — shaded Guava
- [data](data/CLAUDE.md) — Generic record readers/writers
- [parquet](parquet/CLAUDE.md), [orc](orc/CLAUDE.md), [arrow](arrow/CLAUDE.md) — File formats
- [aws](aws/CLAUDE.md), [azure](azure/CLAUDE.md), [gcp](gcp/CLAUDE.md), [aliyun](aliyun/CLAUDE.md), [dell](dell/CLAUDE.md) — Storage / cloud
- [hive-metastore](hive-metastore/CLAUDE.md), [nessie](nessie/CLAUDE.md), [snowflake](snowflake/CLAUDE.md), [bigquery](bigquery/CLAUDE.md) — Catalogs
- [open-api](open-api/CLAUDE.md) — REST Catalog spec
- [spark](spark/CLAUDE.md), [flink](flink/CLAUDE.md), [mr](mr/CLAUDE.md) — Query engines
- [kafka-connect](kafka-connect/CLAUDE.md), [delta-lake](delta-lake/CLAUDE.md) — Streaming / interop
- Bundles: [aws-bundle](aws-bundle/CLAUDE.md), [azure-bundle](azure-bundle/CLAUDE.md), [gcp-bundle](gcp-bundle/CLAUDE.md)
