# iceberg-flink

## Module Overview

`iceberg-flink` is Apache Iceberg's integration with Apache Flink. It provides a
Flink Table API/SQL catalog, a FLIP-27 unified source (batch + streaming reads),
an Iceberg sink (streaming/batch writes with exactly-once commits), table
maintenance operators, and a shaded runtime jar. The code is maintained **per
Flink version**.

## Key Responsibilities

- **Catalog**: expose Iceberg to Flink SQL/Table API (`FlinkCatalog`,
  `FlinkDynamicTableFactory`).
- **Source**: FLIP-27 `IcebergSource` — bounded and unbounded (streaming) reads
  with split enumeration/assignment.
- **Sink**: `IcebergSink`/`FlinkSink` — writers + committer for atomic appends.
- **Maintenance**: in-job table maintenance (compaction, snapshot expiration).

## Version Matrix / Multi-version Layout

Flink support is split by version under `flink/vX.Y/`, each with two subprojects:

```
flink/v1.20/  flink/v2.0/  flink/v2.1/
   ├── flink/          # the integration (catalog, source, sink, data, maintenance)
   └── flink-runtime/  # shaded runtime jar for deployment
```
Gradle projects are named per version, e.g. `iceberg-flink-1.20` and
`iceberg-flink-runtime-1.20`. Known Flink versions: 1.20, 2.0, 2.1. Shared code
is maintained per version — a change usually needs applying to each.

```bash
./gradlew -DflinkVersions=1.20 :iceberg-flink:iceberg-flink-1.20:build
```

## Architecture

### Integration (`org.apache.iceberg.flink`, in `vX.Y/flink`)

```
flink/                 # FlinkCatalog, FlinkDynamicTableFactory, TableLoader,
                       #   FlinkSchemaUtil, FlinkReadConf / FlinkWriteConf, type conversions
flink/source/          # FLIP-27 IcebergSource + FlinkSource (legacy)
flink/source/enumerator/   # Split enumerator (continuous + static)
flink/source/assigner/     # Split assigners (ordering / event-time alignment)
flink/source/reader/       # SourceReader + reader functions (row/avro/parquet/orc)
flink/source/split/        # IcebergSourceSplit + serialization
flink/sink/            # IcebergSink / FlinkSink, writers, committer
flink/sink/shuffle/    # Range/partition shuffle for balanced writes
flink/sink/dynamic/    # Dynamic (multi-table / schema-evolving) sink
flink/data/            # Flink RowData <-> Iceberg readers/writers
flink/maintenance/api/      # Maintenance task API
flink/maintenance/operator/ # Maintenance operators (compaction, expiration)
flink/actions/         # Action helpers (e.g. rewrite)
flink/util/            # Shared utilities
```

### Read path (FLIP-27)
`IcebergSource` → `SplitEnumerator` (plans Iceberg scan into splits) →
`SplitAssigner` (hands splits to readers, supports event-time alignment) →
`SourceReader` (reads `RowData` from data files). Supports both bounded batch
and unbounded streaming (incremental snapshot) reads.

### Write path
`IcebergSink` writers produce data files per checkpoint; the committer commits
them to the table on checkpoint completion, giving exactly-once append
semantics. `sink/shuffle` rebalances/clusters records before writing.

### Runtime (`vX.Y/flink-runtime`)
Shaded fat jar bundling the Flink integration + relocated dependencies for
deployment to a Flink cluster.

## Common Development Tasks

### Registering an Iceberg catalog in Flink SQL
```sql
CREATE CATALOG iceberg WITH (
  'type'='iceberg',
  'catalog-type'='hive',         -- or 'rest', 'hadoop', ...
  'warehouse'='...');
```
This is wired through `FlinkCatalogFactory` / `FlinkDynamicTableFactory`.

### Building a streaming read in DataStream
Use `IcebergSource.forRowData()...streaming(true)...build()` with a
`TableLoader` pointing at the table.

## Dependencies

Per version (e.g. `iceberg-flink-1.20`):

**Iceberg modules — `flink` subproject:**
- `iceberg-api` (`api`), `iceberg-data` (`api`)
- `iceberg-common`, `iceberg-core`, `iceberg-data`, `iceberg-orc`,
  `iceberg-parquet`, `iceberg-hive-metastore` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**`flink-runtime` subproject:** shades the version's `iceberg-flink-X` **and the
cloud/catalog modules** `iceberg-aws`, `iceberg-azure`, `iceberg-gcp`,
`iceberg-bigquery` (`implementation`) into the deployable runtime jar (with many
relocated deps: Avro, Parquet, ORC, Jackson, HttpClient, etc.).

**Depended on by:** nothing internal; the runtime jar is the deployment artifact.

**Key external libs:** Apache Flink (per version), Avro/Parquet/ORC via the
format modules; tests use Hadoop 3 + AWS.

## Module Structure Notes

- Each Flink version is an independent set of buildable subprojects.
- The `flink` subproject compiles against `iceberg-core`, `iceberg-data`,
  `iceberg-parquet`/`-orc`, and `iceberg-hive-metastore`. The **`flink-runtime`**
  jar additionally bundles the cloud modules (`aws`, `azure`, `gcp`, `bigquery`);
  other catalogs (REST/Hadoop/Nessie/…) are selected at runtime via
  `catalog-type`.

Build (example): `./gradlew :iceberg-flink:iceberg-flink-1.20:build`
