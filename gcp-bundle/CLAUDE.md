# iceberg-gcp-bundle

## Module Overview

`iceberg-gcp-bundle` is a shaded "fat jar" that packages the **Google Cloud
client libraries** (and their transitive deps, relocated) into one self-contained
jar. It is the runtime companion to `iceberg-gcp` (and `iceberg-bigquery`): those
modules declare the GCP libraries `compileOnly`, and this bundle supplies the
relocated libraries at runtime so it can be dropped into an engine runtime
(Spark/Flink/Hive) for GCS/BigQuery support without dependency conflicts.

## Key Responsibilities

- **Bundling**: package GCP client libraries (`google-cloud-storage`,
  `google-cloud-bigquery`, `google-cloud-core`, `google-cloud-kms`) into one jar.
- **Relocation**: shade conflicting transitive deps (Jackson, Guava, errorprone,
  gson, protobuf, `org.apache.http`, `io.netty`) for safe coexistence with the
  host engine's classpath.

## Architecture

No application source — the `build.gradle` uses the Gradle Shadow plugin to
assemble and relocate the GCP libraries. The Iceberg logic (GCSFileIO,
BigQueryMetastoreCatalog, …) lives in `iceberg-gcp` / `iceberg-bigquery`,
**not** here; the bundle only carries the (relocated) GCP libraries.

## Common Development Tasks

### Deploying GCP support to an engine
Add the `iceberg-gcp-bundle` jar to the cluster classpath instead of pulling in
`iceberg-gcp` + GCP libraries separately.

## Dependencies

**Iceberg modules:** none (it bundles only the GCP client libraries, not
`iceberg-gcp` / `iceberg-bigquery`).

**Depended on by:** `iceberg-flink` integration tests and `iceberg-open-api`
tests; used directly on engine classpaths as the runtime library companion to
`iceberg-gcp` and `iceberg-bigquery`.

**Key external libs (bundled & relocated):** Google Cloud libraries
(`google-cloud-storage`, `google-cloud-bigquery`, `google-cloud-core`,
`google-cloud-kms`), gcs-analytics-core; relocates Guava, Jackson, gson,
protobuf, errorprone, `org.apache.http`, `io.netty`.

## Module Structure Notes

- Packaging module only; pairs with [gcp](../gcp/CLAUDE.md) and
  [bigquery](../bigquery/CLAUDE.md), which have the GCP libraries as `compileOnly`
  and supply the actual FileIO/catalog code.
- Sibling bundles: [aws-bundle](../aws-bundle/CLAUDE.md),
  [azure-bundle](../azure-bundle/CLAUDE.md).

Build: `./gradlew :iceberg-gcp-bundle:build`
