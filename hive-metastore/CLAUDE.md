# iceberg-hive-metastore

## Module Overview

`iceberg-hive-metastore` integrates the Hive Metastore (HMS) as an Iceberg
catalog. It stores each table's current metadata pointer in HMS and uses HMS
locking to make commits atomic, allowing Iceberg tables to be discovered and
shared through an existing Hive Metastore.

## Key Responsibilities

- **Catalog**: `HiveCatalog` — create/load/drop/rename Iceberg tables in HMS.
- **Commit & locking**: `HiveTableOperations` with HMS-based locking for atomic
  metadata pointer swaps.
- **Client pooling**: pooled, cached Hive metastore Thrift clients.

## Architecture

### HMS integration (`org.apache.iceberg.hive`)

```
HiveCatalog            # Iceberg Catalog backed by Hive Metastore
HiveTableOperations    # refresh()/commit() against HMS; updates the table pointer
HiveClientPool         # Pool of HiveMetaStoreClient connections
CachedClientPool       # Caches client pools per metastore config
HiveCommitLock / MetastoreLock   # HMS-based locking to serialize commits
HiveSchemaUtil         # Iceberg <-> Hive schema conversion
```
The current `metadata_location` table property in HMS is the table pointer; a
commit writes new metadata, then atomically updates that property under lock
(with the previous location as the compare-and-set guard).

## Common Development Tasks

### Using HiveCatalog
Configure `catalog-impl=org.apache.iceberg.hive.HiveCatalog` (or `type=hive`)
with the metastore URI and warehouse location; tables are then registered in HMS.

### Debugging commit conflicts
Look at `HiveTableOperations` + the lock classes — commit retries and lock
acquisition/timeouts are the usual sources of HMS commit issues.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-core` (`implementation`)
- `iceberg-common` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** `iceberg-mr`, `iceberg-spark`, and `iceberg-flink` (they use
`HiveCatalog`); reused widely as the default HMS catalog.

**Key external libs:** Hive metastore Thrift client, Caffeine, Avro
(`compileOnly`); Immutables (annotation processor).

## Module Structure Notes

- Depends on `iceberg-core` and the Hive metastore Thrift client.
- Extends `BaseMetastoreCatalog`/`BaseMetastoreTableOperations` from core.
- Widely reused: the `mr`/Hive runtime and other tools build on `HiveCatalog`.

Build: `./gradlew :iceberg-hive-metastore:build` · Test: `./gradlew :iceberg-hive-metastore:test`
