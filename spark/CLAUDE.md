# iceberg-spark

## Module Overview

`iceberg-spark` is Apache Iceberg's integration with Apache Spark, implemented
against Spark's **DataSource V2** APIs. It provides Iceberg catalogs, scan/write
support, SQL extensions (stored procedures, custom grammar), maintenance
actions, and a shaded runtime jar. The code is maintained **per Spark version**
(and per Scala version).

## Key Responsibilities

- **Catalogs**: expose Iceberg tables to Spark SQL (`SparkCatalog`,
  `SparkSessionCatalog`).
- **Read**: DataSource V2 scans with pushdown, pruning, and vectorized reads.
- **Write**: batch/streaming writes, MERGE/UPDATE/DELETE (copy-on-write &
  merge-on-read).
- **SQL extensions**: stored procedures (`CALL`), `ALTER TABLE ... WRITE`, etc.
- **Maintenance actions**: rewrite data files, expire snapshots, rewrite
  manifests, remove orphan files.

## Version Matrix / Multi-version Layout

Spark support is split by version under `spark/vX.Y/`, each with three
subprojects:

```
spark/v3.4/  spark/v3.5/  spark/v4.0/  spark/v4.1/
   ├── spark/             # core integration (catalog, source, actions, functions)
   ├── spark-extensions/  # SQL extensions: ANTLR grammar, procedures, logical plans
   └── spark-runtime/     # shaded runtime jar for deployment
```
Gradle projects are named per version + Scala suffix, e.g.
`iceberg-spark-3.5_2.12`, `iceberg-spark-extensions-3.5_2.12`,
`iceberg-spark-runtime-3.5_2.12`. Known Spark versions: 3.4, 3.5, 4.0, 4.1;
Scala 2.12 / 2.13 (4.x is 2.13 only). Shared code is templated/copied per
version — a change usually needs applying to each supported version.

```bash
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 \
  :iceberg-spark:iceberg-spark-3.5_2.12:build
```

## Architecture

### Core integration (`org.apache.iceberg.spark`, in `vX.Y/spark`)

```
spark/                 # SparkCatalog, SparkSessionCatalog, SparkSchemaUtil,
                       #   SparkReadConf / SparkWriteConf, type conversions
spark/source/          # DataSource V2: SparkScan, SparkBatch, SparkTable,
                       #   readers/writers (row + vectorized), SparkWrite
spark/source/metrics/  # Custom scan metrics
spark/actions/         # Maintenance actions (RewriteDataFiles, ExpireSnapshots,
                       #   RewriteManifests, DeleteOrphanFiles, RewritePositionDeletes)
spark/data/            # Iceberg <-> Spark data readers/writers
spark/data/vectorized/ # Vectorized (Arrow/Parquet) read path
spark/functions/       # Iceberg SQL functions (bucket, truncate, ...)
spark/procedures/      # Stored procedures invoked via CALL
spark/sql/             # SQL helpers
```

### SQL extensions (`vX.Y/spark-extensions`)

```
org.apache.spark.sql.catalyst...           # Extended logical plans / analysis rules
org.apache.spark.sql.connector.iceberg...  # Connector extension interfaces
org.apache.iceberg.spark.extensions        # Extension entry points
ANTLR grammar                               # Iceberg-specific SQL (CALL, etc.)
```
Enabled via Spark's `spark.sql.extensions=...IcebergSparkSessionExtensions`.

### Runtime (`vX.Y/spark-runtime`)

Shaded fat jar bundling the Spark integration + relocated dependencies, for
adding Iceberg to a Spark cluster without conflicts.

## Common Development Tasks

### Registering an Iceberg catalog in Spark
Set `spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog` (plus
catalog `type`/`warehouse`); use `SparkSessionCatalog` to also wrap the built-in
session catalog.

### Adding a stored procedure
Implement it in `spark/procedures/` for each supported version and register it;
expose syntax via the `spark-extensions` grammar if needed.

## Dependencies

Per version (e.g. `iceberg-spark-3.5_2.12`):

**Iceberg modules — `spark` subproject:**
- `iceberg-api` (`api`)
- `iceberg-common`, `iceberg-core`, `iceberg-data`, `iceberg-orc`,
  `iceberg-parquet`, `iceberg-arrow` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)
- `iceberg-hive-metastore` (`testImplementation` only — not a compile dep)

**`spark-extensions` subproject:** `iceberg-api`, `iceberg-core`,
`iceberg-common`, and the version's `iceberg-spark-X` (all `compileOnly`).

**`spark-runtime` subproject:** `iceberg-api` (`api`) plus `implementation` of
the version's `iceberg-spark-X` and `iceberg-spark-extensions-X` **and the
catalog/cloud modules** `iceberg-aws`, `iceberg-azure`, `iceberg-gcp`,
`iceberg-bigquery`, `iceberg-hive-metastore` — all shaded (with relocated deps)
into the deployable runtime jar.

**Depended on by:** nothing internal; the runtime jar is the deployment artifact.

**Key external libs:** Apache Spark, Parquet, Arrow, ORC, RoaringBitmap,
Caffeine.

## Module Structure Notes

- Each Spark version is an independent set of buildable subprojects.
- The `spark` subproject compiles against `iceberg-core`, `iceberg-data`, and
  `iceberg-parquet`/`-orc`/`-arrow`; `iceberg-hive-metastore` is a test-only dep
  there. The **`spark-runtime`** jar is what bundles the cloud + Hive catalog
  modules (`aws`, `azure`, `gcp`, `bigquery`, `hive-metastore`) for deployment.
- Integration tests (often Docker-backed) live alongside unit tests per version.

Build (example): `./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:build`
