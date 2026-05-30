# iceberg-azure

## Module Overview

`iceberg-azure` provides Microsoft Azure integration for Iceberg, primarily an
Iceberg `FileIO` over Azure Data Lake Storage Gen2 (ADLS Gen2 / `abfss`), plus
Azure-based key management. It builds on the Azure SDK for Java.

## Key Responsibilities

- **Storage**: `ADLSFileIO` — Iceberg `FileIO` over ADLS Gen2.
- **Key management**: Azure-backed encryption key management.
- **Configuration**: Azure credentials/endpoints via property maps.

## Architecture

### Storage — ADLS Gen2 (`org.apache.iceberg.azure.adlsv2`)

```
ADLSFileIO         # FileIO over ADLS Gen2
ADLSLocation       # Parse/represent abfss:// locations (account/container/path)
ADLSInputFile / ADLSOutputFile / ADLSInputStream / ADLSOutputStream
```

### Configuration & key management

```
org.apache.iceberg.azure.AzureProperties      # Account keys, SAS tokens, AAD creds, endpoints
org.apache.iceberg.azure.keymanagement        # Azure Key Vault-based key management
```
Azure behavior is configured through catalog/table property maps resolved into
`AzureProperties` (e.g. storage account name, shared-key or token credentials).

## Common Development Tasks

### Using ADLSFileIO
Set `io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO` on the catalog and
supply the relevant Azure storage properties; tables then read/write data on
ADLS Gen2 using `abfss://` locations.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-common` (`implementation`)
- `iceberg-core` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal. Used at runtime by any engine reading ADLS
Gen2 tables; pair it with `iceberg-azure-bundle`, which supplies the relocated
Azure SDK this module compiles against (`compileOnly`).

**Key external libs:** Azure SDK (`azure-storage-file-datalake`,
`azure-security-keyvault-keys`, `azure-identity` — `compileOnly`/runtime via the
bundle).

## Module Structure Notes

- Depends on `iceberg-core`/`iceberg-api` and the Azure SDK; ships as
  `iceberg-azure` (see `iceberg-azure-bundle` for a shaded variant).
- Implements the `FileIO`/`InputFile`/`OutputFile` contracts from api/core.

Build: `./gradlew :iceberg-azure:build` · Test: `./gradlew :iceberg-azure:test`
