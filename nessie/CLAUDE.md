# iceberg-nessie

## Module Overview

`iceberg-nessie` integrates Project Nessie — a Git-like, versioned data catalog
— as an Iceberg catalog. It lets Iceberg tables live on Nessie branches and tags
with commit-based version control, enabling multi-table transactions, isolated
branches, and time travel across the whole catalog.

## Key Responsibilities

- **Catalog**: `NessieCatalog` — manage Iceberg tables on Nessie references.
- **Commit**: `NessieTableOperations` — commit metadata changes as Nessie commits.
- **Versioning**: operate against a branch/tag/hash reference.

## Architecture

### Nessie integration (`org.apache.iceberg.nessie`)

```
NessieCatalog            # Iceberg Catalog backed by a Nessie server
NessieTableOperations    # refresh()/commit() as Nessie content commits
NessieIcebergClient      # Wraps the Nessie API client; resolves references
NessieUtil               # Helpers for content/reference handling
```
Each Iceberg table is a Nessie "content" object on a reference; committing a new
metadata location is a Nessie commit, so the catalog inherits Nessie's atomicity
and branching semantics.

## Common Development Tasks

### Using NessieCatalog
Configure `catalog-impl=org.apache.iceberg.nessie.NessieCatalog` with the Nessie
endpoint URI, the default `ref` (branch), and warehouse location.

### Working with branches/tags
Point the catalog at a branch or tag via the `ref` property to read/write an
isolated version of the catalog.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-common` (`implementation`)
- `iceberg-core` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal (leaf catalog module); selected at runtime
by engines via `catalog-impl`.

**Key external libs:** Nessie client, Jackson, Hadoop common (`compileOnly`),
MicroProfile OpenAPI (`compileOnly`).

## Module Structure Notes

- Depends on `iceberg-core` and the Nessie client libraries.
- Extends the metastore catalog base classes from core.

Build: `./gradlew :iceberg-nessie:build` · Test: `./gradlew :iceberg-nessie:test`
