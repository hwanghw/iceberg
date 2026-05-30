# iceberg-bigquery

## Module Overview

`iceberg-bigquery` provides an Iceberg catalog backed by the BigQuery Metastore,
letting engines manage and load Iceberg tables that are registered in Google
BigQuery's metastore. Its code lives under the `org.apache.iceberg.gcp.bigquery`
package (shared lineage with the `gcp` module).

## Key Responsibilities

- **Catalog**: `BigQueryMetastoreCatalog` — create/load/list/drop Iceberg tables
  registered in BigQuery Metastore.
- **Metadata access**: a client wrapping the BigQuery API to read/update table
  metadata pointers.

## Architecture

### BigQuery Metastore integration (`org.apache.iceberg.gcp.bigquery`)

```
BigQueryMetastoreCatalog       # Iceberg Catalog over BigQuery Metastore
BigQueryTableOperations        # refresh()/commit() against BigQuery metadata
BigQueryMetastoreClient        # Wraps BigQuery API calls
```
Table metadata pointers are stored/updated through the BigQuery Metastore; the
catalog then loads standard Iceberg metadata/manifests via `FileIO` (typically
`GCSFileIO` from `iceberg-gcp`).

## Common Development Tasks

### Using the BigQuery catalog
Configure `catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog`
with the GCP project, dataset/location, and GCS warehouse properties.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-common` (`implementation`)
- `iceberg-core` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

> Note: it does **not** depend on `iceberg-gcp` at the module level even though
> it shares the `org.apache.iceberg.gcp` package root.

**Depended on by:** nothing internal. Pair it with `iceberg-gcp-bundle`, which
supplies the relocated GCP/BigQuery libraries this module compiles against.

**Key external libs:** Google BigQuery client (`google-cloud-bigquery`,
`google-cloud-core`), GCS (`compileOnly`).

## Module Structure Notes

- Depends on `iceberg-core` and Google BigQuery client libraries; closely
  related to [gcp](../gcp/CLAUDE.md) (shares the `org.apache.iceberg.gcp`
  package root and typically uses `GCSFileIO`).
- Extends the metastore catalog base classes from core.

Build: `./gradlew :iceberg-bigquery:build` · Test: `./gradlew :iceberg-bigquery:test`
