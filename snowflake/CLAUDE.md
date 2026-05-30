# iceberg-snowflake

## Module Overview

`iceberg-snowflake` provides a (read-only) Iceberg catalog backed by Snowflake.
It lets engines load Snowflake-managed Iceberg tables by querying Snowflake's
metadata over JDBC and resolving the underlying Iceberg metadata files.

## Key Responsibilities

- **Catalog**: `SnowflakeCatalog` — list/load Snowflake-managed Iceberg tables.
- **Metadata access**: query Snowflake (via JDBC) to find each table's current
  Iceberg metadata location.

## Architecture

### Snowflake integration (`org.apache.iceberg.snowflake`)

```
SnowflakeCatalog          # Iceberg Catalog over Snowflake (read/load oriented)
SnowflakeTableOperations  # Resolves the current Iceberg metadata for a table
JdbcSnowflakeClient       # JDBC client issuing Snowflake metadata queries
SnowflakeIdentifier       # Database/schema/table identifier handling
NamespaceHelpers          # Map Iceberg namespaces to Snowflake db/schema
```
The catalog asks Snowflake for a table's metadata-file location, then loads
standard Iceberg metadata/manifests through `FileIO`.

## Common Development Tasks

### Using SnowflakeCatalog
Configure `catalog-impl=org.apache.iceberg.snowflake.SnowflakeCatalog` with the
Snowflake JDBC URI and credentials, plus an appropriate `FileIO` for the storage
holding the data/metadata files.

## Dependencies

**Iceberg modules:**
- `iceberg-core` (`implementation` — pulls in `iceberg-api` transitively)
- `iceberg-common` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal (leaf catalog module).

**Key external libs:** Snowflake JDBC driver (`runtimeOnly`), Jackson.

## Module Structure Notes

- Depends on `iceberg-core` and the Snowflake JDBC driver.
- Primarily read/load oriented — Snowflake owns table lifecycle/commits.

Build: `./gradlew :iceberg-snowflake:build` · Test: `./gradlew :iceberg-snowflake:test`
