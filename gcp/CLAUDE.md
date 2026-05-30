# iceberg-gcp

## Module Overview

`iceberg-gcp` provides Google Cloud Platform integration for Iceberg, primarily
an Iceberg `FileIO` over Google Cloud Storage (`GCSFileIO`), plus GCP
authentication helpers. It builds on the Google Cloud Java libraries. (The
BigQuery Metastore catalog, which shares the `org.apache.iceberg.gcp` lineage,
lives in the separate `bigquery` module.)

## Key Responsibilities

- **Storage**: `GCSFileIO` — Iceberg `FileIO` over Google Cloud Storage.
- **Auth**: GCP credential/token helpers.
- **Configuration**: GCP project/credentials/endpoints via property maps.

## Architecture

### Storage — GCS (`org.apache.iceberg.gcp.gcs`)

```
GCSFileIO          # FileIO over Google Cloud Storage
GCSInputFile / GCSOutputFile / GCSInputStream / GCSOutputStream
```

### Auth & configuration

```
org.apache.iceberg.gcp.GCPProperties   # Project ID, credentials, GCS endpoint/options
org.apache.iceberg.gcp.auth            # OAuth2 / credential helpers
```
GCP behavior is configured through catalog/table property maps resolved into
`GCPProperties`.

## Common Development Tasks

### Using GCSFileIO
Set `io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO` on the catalog with the
relevant GCP properties; tables then read/write data on GCS using `gs://`
locations.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-common` (`implementation`)
- `iceberg-core` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal. Used at runtime by any engine reading GCS
tables; pair it with `iceberg-gcp-bundle`, which supplies the relocated GCP
libraries this module compiles against (`compileOnly`).

**Key external libs:** Google Cloud libraries (`google-cloud-storage`,
`google-cloud-kms` — `compileOnly`/runtime via the bundle).

## Module Structure Notes

- Depends on `iceberg-core`/`iceberg-api` and the GCP client libraries; ships as
  `iceberg-gcp` (see `iceberg-gcp-bundle` for a shaded variant).
- Implements the `FileIO`/`InputFile`/`OutputFile` contracts from api/core.
- Related: the `bigquery` module provides the BigQuery Metastore catalog under
  `org.apache.iceberg.gcp.bigquery`.

Build: `./gradlew :iceberg-gcp:build` · Test: `./gradlew :iceberg-gcp:test`
