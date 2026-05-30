# iceberg-core

## Module Overview

`iceberg-core` is the reference implementation of the Iceberg table format
defined by `iceberg-api`. It implements table metadata, the snapshot/manifest
machinery, the atomic commit protocol, the built-in catalogs (REST, JDBC,
Hadoop, plus the base class shared by metastore catalogs), metadata tables, and
Avro/Puffin support. It is engine-independent and is **what processing engines
should depend on**.

## Key Responsibilities

- **Table metadata**: read/write/evolve `TableMetadata` and its JSON encoding
  (format versions V1–V4).
- **Snapshots & manifests**: produce snapshots; read/write manifest lists and
  manifest files with column statistics.
- **Commit protocol**: optimistic-concurrency, atomic metadata swaps via
  `TableOperations`.
- **Catalogs**: REST, JDBC, Hadoop, caching, and the metastore base class.
- **Metadata tables**: snapshots, manifests, files, history, partitions, etc.
- **Format support**: Avro read/write (metadata + data), Puffin stats files.

## Architecture

### Metadata & commit (`org.apache.iceberg`)

```
TableMetadata / TableMetadataParser    # In-memory metadata + JSON (de)serialization
V1Metadata / V2Metadata / V3Metadata / V4Metadata   # Per-format-version encodings
BaseTable / SerializableTable          # Table implementation
BaseMetastoreCatalog / BaseMetastoreTableOperations  # Base for HMS-style catalogs
BaseTransaction                        # Multi-op atomic transaction
TableOperations (api)                  # refresh() + commit(base, metadata): atomic swap
CatalogUtil / CatalogProperties        # Catalog loading & config helpers
CachingCatalog                         # Table-cache wrapper
```
A commit builds a new `TableMetadata`, writes a new metadata file, and asks
`TableOperations.commit()` to atomically replace the current pointer; on
conflict the operation re-reads the base and retries (optimistic concurrency).

### Snapshots & manifests

```
SnapshotProducer        # Base for all snapshot-creating operations
FastAppend / MergeAppend (Base*Append) # Append strategies (new manifest vs. merge)
BaseOverwriteFiles / BaseRowDelta / BaseRewriteFiles / BaseReplacePartitions
BaseRewriteManifests / CherryPickOperation
SnapshotManager         # Branch/tag/rollback management
ManifestWriter / ManifestReader / ManifestFiles / ManifestListWriter
BaseSnapshot / DeleteFileIndex
```
Read-time hierarchy: `Snapshot → manifest list → manifest files → data / delete
files`. Manifests carry per-column bounds/null counts that drive
metadata-only scan planning and predicate pushdown.

### Scans & metadata tables

```
BaseScan / BaseTableScan / DataScan / SnapshotScan / BaseIncrementalScan
*Table classes: SnapshotsTable, ManifestsTable, AllManifestsTable, FilesTable,
  DataFilesTable, DeleteFilesTable, PartitionsTable, HistoryTable, RefsTable,
  ManifestEntriesTable, PositionDeletesTable, MetadataLogEntriesTable, ...
```

### Catalogs

```
org.apache.iceberg.rest      # RESTCatalog, RESTSessionCatalog, HTTPClient,
                             #   RESTTableOperations, requests/, responses/, auth/, credentials/
org.apache.iceberg.jdbc      # JdbcCatalog (metadata pointers in a JDBC database)
org.apache.iceberg.hadoop    # HadoopCatalog, HadoopTables, HadoopFileIO (filesystem-only)
org.apache.iceberg.inmemory  # In-memory catalog & FileIO for tests
```
`RESTCatalog` is the client side of the Iceberg REST Catalog protocol (spec in
`iceberg-open-api`). `BaseMetastoreCatalog` is reused by `iceberg-hive-metastore`,
Glue, Nessie, etc.

### Formats & encoding

```
org.apache.iceberg.avro      # Avro value readers/writers (manifests + data)
org.apache.iceberg.puffin    # Puffin file format for stats/sketches (e.g. NDV blobs)
org.apache.iceberg.deletes   # Position/equality delete writers & readers
org.apache.iceberg.mapping   # Name mapping (schema-less file → field IDs)
org.apache.iceberg.encryption# Encryption manager implementations
org.apache.iceberg.io         # FileIO helpers, rolling/append writers
org.apache.iceberg.schema     # Schema update/visitor implementations
org.apache.iceberg.metrics    # Metrics reporters (scan/commit reports)
org.apache.iceberg.view       # View metadata implementation
```

## Common Development Tasks

### Implementing a new catalog
Extend `BaseMetastoreCatalog` and implement `TableOperations` (`refresh()` +
atomic `commit()`); the commit's atomicity is the catalog's core contract.

### Adding a snapshot operation
Extend `SnapshotProducer`, build the manifests, define the snapshot summary;
wire the operation in from `BaseTable`/`BaseTransaction`.

### Reading/writing manifests
Use `ManifestFiles` / `ManifestReader` / `ManifestWriter` rather than touching
Avro directly.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api` — re-exported to consumers)
- `iceberg-common` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)
- `iceberg-parquet` (`testRuntimeOnly` — for tests only)

**Depended on by:** every module above the foundation layer — the formats
(`data`, `parquet`, `orc`, `arrow`), all cloud/`FileIO` modules, all catalog
modules, and all engine integrations. This is the jar processing engines should
depend on.

**Key external libs:** Jackson (JSON), Caffeine (caching), Avro (metadata/data),
Apache HttpClient 5 (REST client), RoaringBitmap (position deletes),
aircompressor; Immutables (annotation processor).

## Module Structure Notes

- Depends only on `iceberg-api` + `iceberg-common`; **no engine or cloud deps**.
- File formats Parquet/ORC/Arrow live in their own modules — core uses Avro for
  metadata and provides generic IO hooks the format modules plug into.
- JUnit 5 tests; many use `inmemory`/`hadoop` catalogs and temp dirs.

Build: `./gradlew :iceberg-core:build` · Test: `./gradlew :iceberg-core:test`
