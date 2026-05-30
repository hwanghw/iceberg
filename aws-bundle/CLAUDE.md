# iceberg-aws-bundle

## Module Overview

`iceberg-aws-bundle` is a shaded "fat jar" that packages `iceberg-aws` together
with the AWS SDK for Java v2 and relocates their transitive dependencies. It is
meant to be dropped into an engine runtime (Spark/Flink/Hive) to add AWS support
(S3, Glue, etc.) without dependency conflicts.

## Key Responsibilities

- **Bundling**: combine `iceberg-aws` + AWS SDK v2 clients (S3, Glue, STS,
  DynamoDB, KMS) into one self-contained jar.
- **Relocation**: shade conflicting transitive deps so the bundle coexists with
  whatever the host engine already ships.

## Architecture

No application source — the `build.gradle` uses the Gradle Shadow plugin to
assemble and relocate dependencies into a single publishable artifact. The
actual logic (S3FileIO, GlueCatalog, …) lives in `iceberg-aws`.

## Common Development Tasks

### Deploying AWS support to an engine
Add the `iceberg-aws-bundle` jar (alongside the engine's iceberg runtime jar) to
the cluster classpath instead of pulling in `iceberg-aws` + AWS SDK separately.

## Dependencies

**Iceberg modules:** none (it bundles only the AWS SDK, not `iceberg-aws`).

**Depended on by:** `iceberg-flink` integration tests and `iceberg-open-api`
tests; used directly on engine classpaths as the runtime SDK companion to
`iceberg-aws`.

**Key external libs (bundled & relocated):** AWS SDK v2 (S3, Glue, STS,
DynamoDB, KMS, LakeFormation, IAM, SSO), analytics-accelerator-s3; relocates
`org.apache.http` and `io.netty`.

## Module Structure Notes

- Packaging module only; pairs with [aws](../aws/CLAUDE.md), which has the AWS SDK
  as `compileOnly` and supplies the actual FileIO/catalog code.
- Sibling bundles: [azure-bundle](../azure-bundle/CLAUDE.md),
  [gcp-bundle](../gcp-bundle/CLAUDE.md).

Build: `./gradlew :iceberg-aws-bundle:build`
