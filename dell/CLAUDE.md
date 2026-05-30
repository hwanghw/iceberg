# iceberg-dell

## Module Overview

`iceberg-dell` integrates Dell EMC ECS (Elastic Cloud Storage) object storage
with Iceberg. It provides both an Iceberg `FileIO` over ECS and an ECS-backed
catalog implementation.

## Key Responsibilities

- **Storage**: `EcsFileIO` — Iceberg `FileIO` over Dell ECS object storage.
- **Catalog**: `EcsCatalog` — store and atomically update table metadata in ECS.

## Architecture

### ECS integration (`org.apache.iceberg.dell.ecs`)

```
EcsFileIO              # FileIO over Dell ECS
EcsCatalog             # ECS-backed Iceberg catalog
EcsTableOperations     # Atomic metadata commit on ECS
BaseEcsFile / EcsInputFile / EcsInputStream / EcsOutputFile
EcsURI                 # ECS object location parsing
```
`EcsCatalog`/`EcsTableOperations` rely on ECS object-store semantics (e.g.
conditional/if-match updates) to implement Iceberg's atomic metadata swap.

## Common Development Tasks

### Using the ECS catalog
Configure `catalog-impl=org.apache.iceberg.dell.ecs.EcsCatalog` and ECS endpoint
/credential properties; data IO uses `EcsFileIO`.

## Dependencies

**Iceberg modules:**
- `iceberg-core` (`implementation` — pulls in `iceberg-api` transitively)
- `iceberg-common` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal (leaf cloud/catalog module); used at runtime
by engines reading ECS tables.

**Key external libs:** Dell ECS object client (`object-client-bundle`,
`compileOnly`).

## Module Structure Notes

- Depends on `iceberg-core`/`iceberg-api` and the Dell ECS client SDK.
- Implements the `FileIO` and `Catalog`/`TableOperations` contracts from api/core.
- Includes an `ecs/mock` test harness for unit testing without a real ECS.

Build: `./gradlew :iceberg-dell:build` · Test: `./gradlew :iceberg-dell:test`
