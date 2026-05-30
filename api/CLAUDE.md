# iceberg-api

## Module Overview

`iceberg-api` defines Apache Iceberg's public, stable programming interface. It
is almost entirely interfaces and value types — the contract that engines and
tools code against — with the concrete implementation living in `iceberg-core`.
This is the module to read first to understand the Iceberg data model.

## Key Responsibilities

- **Table abstraction**: `Table`, `Transaction`, and the operation builders that
  mutate tables (append, overwrite, delete, rewrite, schema/spec evolution).
- **Type system & schema**: field-ID-based `Schema`, `Types`, and conversions.
- **Expressions**: predicate/expression tree used for filtering and pruning.
- **Partitioning**: `PartitionSpec` and partition `transforms` (hidden partitioning).
- **Catalog interface**: `Catalog`, `Namespace`, `TableIdentifier`, views.
- **Storage abstraction**: `FileIO`, `InputFile`, `OutputFile`.

## Architecture

### Table model (`org.apache.iceberg`)

```
Table              # Entry point: schema(), spec(), currentSnapshot(), newScan(), io()
Transaction        # Groups multiple operations into one atomic commit
Snapshot           # Immutable table state; enables time travel & isolation
DataFile/DeleteFile/ManifestFile/ManifestListFile  # Physical layout descriptors with stats
Metrics            # Per-file column stats used for scan planning
```

Mutation operations (each is a builder committed via `commit()`):
```
AppendFiles / RewriteFiles / OverwriteFiles / RowDelta /
ReplacePartitions / DeleteFiles / ExpireSnapshots / ManageSnapshots /
UpdateSchema / UpdatePartitionSpec / ReplaceSortOrder / UpdateProperties /
UpdateStatistics / UpdatePartitionStatistics / UpdateLocation
```

### Scanning

```
Scan / TableScan / BatchScan        # Plan files for a read
IncrementalAppendScan / IncrementalChangelogScan   # Incremental reads
  → FileScanTask / ScanTask          # A data file + applicable delete files + residual filter
  → CombinedScanTask / ScanTaskGroup # Grouped tasks for execution
```

### Catalog (`org.apache.iceberg.catalog`)

```
Catalog            # createTable, loadTable, renameTable, dropTable, listTables
SupportsNamespaces # Namespace create/list/drop
Namespace / TableIdentifier
SessionCatalog     # Catalog variant carrying session context
ViewCatalog / ViewSessionCatalog  # View support
```

### Type system & schema (`org.apache.iceberg.types`, `org.apache.iceberg`)

- `Schema` is built from `Types.NestedField`s, each with a **unique field ID**;
  schema evolution (add/drop/rename/reorder) tracks columns by ID, never by name
  or position.
- `Type` / `Types` model primitives and nested types (struct, list, map).

### Expressions & transforms

```
org.apache.iceberg.expressions   # Expression, Expressions, predicates, Literal, Binder,
                                 #   Evaluator, InclusiveMetricsEvaluator, ResidualEvaluator
org.apache.iceberg.transforms    # bucket, truncate, year/month/day/hour, identity
```
Transforms implement **hidden partitioning**: partition values are derived from
source columns, so queries filter on raw columns and Iceberg prunes partitions.

### Storage abstraction (`org.apache.iceberg.io`)

```
FileIO        # Pluggable file access (newInputFile/newOutputFile/deleteFile)
InputFile / OutputFile / SeekableInputStream / PositionOutputStream
```
Implementations live in the cloud modules (`iceberg-aws`, `-azure`, `-gcp`,
`-aliyun`, `-dell`) and `iceberg-core` (Hadoop/local/in-memory).

### Other packages

```
org.apache.iceberg.actions      # Action interfaces (maintenance) — impls in engines
org.apache.iceberg.encryption   # Encryption key management interfaces
org.apache.iceberg.metrics      # Metrics reporting (scan/commit reports)
org.apache.iceberg.view         # View metadata model
org.apache.iceberg.variants     # Variant (semi-structured) type support
org.apache.iceberg.geospatial   # Geospatial type support
org.apache.iceberg.events       # Listener events
org.apache.iceberg.exceptions   # Public exception hierarchy
org.apache.iceberg.util         # Shared utilities
```

## Common Development Tasks

### Adding a new table operation
Define the builder interface here (extending `PendingUpdate`/`SnapshotUpdate`),
then implement it in `iceberg-core` and surface it from `Table`.

### Reading the data model
Start at `Table`, follow to `Snapshot` → `ManifestFile` → `DataFile`, and read
`Schema`/`PartitionSpec` for the metadata side.

## Dependencies

**Iceberg modules:**
- `iceberg-bundled-guava` (relocated Guava) — its only internal dependency.

**Depended on by:** essentially every other module (directly or transitively) —
`iceberg-core` first, then all formats, cloud, catalog, and engine modules.

**Key external libs:** none at runtime (only `compileOnly` annotations:
Error Prone, JSR-305).

## Module Structure Notes

- Dependency root: `iceberg-core` and everything above it depend on it; it
  depends only on `iceberg-common` / `iceberg-bundled-guava` plus minimal deps.
- Keep additions backward compatible — this is a public, stable API surface.

Build: `./gradlew :iceberg-api:build` · Test: `./gradlew :iceberg-api:test`
