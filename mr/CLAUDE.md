# iceberg-mr

## Module Overview

`iceberg-mr` integrates Iceberg with Hadoop MapReduce. It provides a MapReduce
`InputFormat` (and a legacy `mapred` wrapper) for reading Iceberg tables, plus
configuration helpers that resolve Iceberg catalogs/tables from Hadoop config.
It is the building block historically used to add Iceberg read support to
Hadoop/Hive-style execution.

## Key Responsibilities

- **MapReduce reads**: `IcebergInputFormat` and the split/record-reader machinery
  that turns an Iceberg scan into Hadoop input splits.
- **Legacy `mapred` support**: a wrapper exposing the same reads via the old
  `org.apache.hadoop.mapred` API.
- **Configuration**: catalog/table resolution from Hadoop configuration.

## Architecture

### Top level (`org.apache.iceberg.mr`)

```
Catalogs               # Resolve Iceberg catalogs/tables from Hadoop configuration
InputFormatConfig      # Configuration keys + helpers for the input formats
```

### MapReduce API (`org.apache.iceberg.mr.mapreduce`)

```
IcebergInputFormat                    # org.apache.hadoop.mapreduce InputFormat over a table
IcebergSplit / IcebergSplitContainer  # Splits wrapping Iceberg scan tasks
```

### Legacy mapred (`org.apache.iceberg.mr.mapred`)

```
MapredIcebergInputFormat              # org.apache.hadoop.mapred wrapper
AbstractMapredIcebergRecordReader     # Record-reader base
Container                             # Value container for the mapred API
```

The input formats plan an Iceberg `TableScan` (using `Catalogs` /
`InputFormatConfig` to locate the table and apply projection/filters), then emit
one Hadoop split per Iceberg scan task; the record readers decode rows via the
generic `iceberg-data` readers.

## Common Development Tasks

### Reading an Iceberg table in a MapReduce job
Configure `IcebergInputFormat` via `InputFormatConfig` (catalog, table name,
projected schema, filters) and run a standard MapReduce job; use
`MapredIcebergInputFormat` for the legacy `mapred` API.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-data` (`api`)
- `iceberg-common` (`implementation`)
- `iceberg-core` (`implementation`)
- `iceberg-hive-metastore` (`implementation` — for `HiveCatalog` resolution)
- `iceberg-orc` (`implementation`)
- `iceberg-parquet` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal.

**Key external libs:** Hadoop 3 client (`compileOnly`), Caffeine, Parquet
column, ORC (nohive classifier); tests use Hive 2 service/metastore, Calcite,
Tez.

## Module Structure Notes

- Depends on `iceberg-core`, `iceberg-data`, the format modules, and
  `iceberg-hive-metastore` (for catalog resolution).
- This checkout's main source contains the `mr`, `mr.mapred`, and `mr.mapreduce`
  packages (read-oriented MapReduce integration). Engines like Spark and Flink
  have their own dedicated modules for richer integration.

Build: `./gradlew :iceberg-mr:build` · Test: `./gradlew :iceberg-mr:test`
