# iceberg-azure-bundle

## Module Overview

`iceberg-azure-bundle` is a shaded "fat jar" that packages the **Azure SDK**
(and its transitive deps, relocated) into one self-contained jar. It is the
runtime companion to `iceberg-azure`: that module declares the Azure SDK
`compileOnly`, and this bundle supplies the relocated SDK at runtime so it can be
dropped into an engine runtime (Spark/Flink/Hive) for ADLS Gen2 support without
dependency conflicts.

## Key Responsibilities

- **Bundling**: package the Azure SDK (`azure-storage-file-datalake`,
  `azure-security-keyvault-keys`, `azure-identity`) into one jar.
- **Relocation**: shade conflicting transitive deps (`io.netty`,
  `com.fasterxml.jackson`) for safe coexistence with the host engine's classpath.

## Architecture

No application source — the `build.gradle` uses the Gradle Shadow plugin to
assemble and relocate the Azure SDK. The Iceberg logic (ADLSFileIO, …) lives in
`iceberg-azure`, **not** here; the bundle only carries the (relocated) SDK.

## Common Development Tasks

### Deploying Azure support to an engine
Add the `iceberg-azure-bundle` jar to the cluster classpath instead of pulling
in `iceberg-azure` + Azure SDK separately.

## Dependencies

**Iceberg modules:** none (it bundles only the Azure SDK, not `iceberg-azure`).

**Depended on by:** `iceberg-flink` integration tests and `iceberg-open-api`
tests; used directly on engine classpaths as the runtime SDK companion to
`iceberg-azure`.

**Key external libs (bundled & relocated):** Azure SDK
(`azure-storage-file-datalake`, `azure-security-keyvault-keys`,
`azure-identity`); relocates `io.netty` and `com.fasterxml.jackson`.

## Module Structure Notes

- Packaging module only; pairs with [azure](../azure/CLAUDE.md), which has the
  Azure SDK as `compileOnly` and supplies the actual FileIO code.
- Sibling bundles: [aws-bundle](../aws-bundle/CLAUDE.md),
  [gcp-bundle](../gcp-bundle/CLAUDE.md).

Build: `./gradlew :iceberg-azure-bundle:build`
