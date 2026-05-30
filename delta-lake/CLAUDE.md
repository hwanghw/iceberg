# iceberg-delta-lake

## Module Overview

`iceberg-delta-lake` provides Delta Lake → Iceberg interoperability: an action
that **snapshots/migrates** an existing Delta Lake table into an Iceberg table by
reading the Delta transaction log and registering the existing Parquet data files
as an Iceberg snapshot (no data rewrite).

## Key Responsibilities

- **Migration action**: convert a Delta Lake table into an Iceberg table.
- **Delta log reading**: interpret the Delta transaction log to enumerate data
  files and schema.
- **Iceberg registration**: build Iceberg metadata/snapshot referencing the
  Delta table's existing files.

## Architecture

### Delta interop (`org.apache.iceberg.delta`)

```
DeltaLakeToIcebergMigrationActionsProvider   # Entry point to obtain the migration action
SnapshotDeltaLakeTable                       # Action interface: snapshot a Delta table as Iceberg
BaseSnapshotDeltaLakeTableAction             # Implementation: reads Delta log, builds Iceberg snapshot
```
The action reads the Delta table's transaction log (via the Delta Standalone /
Kernel library), maps Delta schema and partitioning to Iceberg, and commits an
Iceberg snapshot that points at the Delta table's existing Parquet files.

## Common Development Tasks

### Snapshotting a Delta table as Iceberg
```java
DeltaLakeToIcebergMigrationActionsProvider.defaultActions()
    .snapshotDeltaLakeTable(deltaTableLocation)
    .as(TableIdentifier.of("db", "tbl"))
    .icebergCatalog(catalog)
    .deltaLakeConfiguration(hadoopConf)
    .execute();
```

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-common` (`implementation`)
- `iceberg-core` (`implementation`)
- `iceberg-parquet` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal (leaf interop module).

**Key external libs:** Delta Standalone (`io.delta:delta-standalone`,
`compileOnly`), Jackson; Immutables (annotation processor). Netty is excluded so
it only comes from `iceberg-arrow` when present.

## Module Structure Notes

- Depends on `iceberg-core` (+ `iceberg-parquet`) and a Delta Lake reader
  library.
- Migration-oriented: it bridges an existing Delta table into Iceberg rather than
  providing a general Delta read/write engine integration.

Build: `./gradlew :iceberg-delta-lake:build` · Test: `./gradlew :iceberg-delta-lake:test`
