# iceberg-aws

## Module Overview

`iceberg-aws` provides Amazon Web Services integration for Iceberg: object
storage via S3 (`S3FileIO`), catalogs backed by AWS Glue and DynamoDB,
LakeFormation-based access control, S3 request signing, and the AWS SDK client
factory/configuration layer. It builds on the AWS SDK for Java v2.

## Key Responsibilities

- **Storage**: `S3FileIO` — Iceberg `FileIO` over Amazon S3.
- **Catalogs**: `GlueCatalog` (AWS Glue Data Catalog) and `DynamoDbCatalog`.
- **Locking**: DynamoDB-based commit lock manager.
- **Security**: LakeFormation integration and S3 request signing (SigV4).
- **Client config**: pluggable AWS client factories and property classes.

## Architecture

### Storage — S3 (`org.apache.iceberg.aws.s3`)

```
S3FileIO            # FileIO over S3 (newInputFile/newOutputFile/deleteFile, bulk delete)
S3InputFile / S3OutputFile / S3InputStream / S3OutputStream
S3RequestUtil       # Request configuration (SSE, ACLs, etc.)
s3/signer/          # S3 V4 request signing support
```

### Catalogs (`org.apache.iceberg.aws.glue`, `…dynamodb`)

```
glue/GlueCatalog + GlueTableOperations      # Glue Data Catalog as an Iceberg catalog
dynamodb/DynamoDbCatalog + DynamoDbTableOperations
dynamodb/DynamoDbLockManager                # Commit locking via DynamoDB
```

### Access control & signing

```
lakeformation/      # AWS LakeFormation credential/permission integration
RESTSigV4*Signer    # SigV4 signing for the REST catalog client
```

### Client configuration (`org.apache.iceberg.aws`)

```
AwsClientFactory / AwsClientFactories       # Build S3/Glue/DynamoDB/KMS clients
AssumeRoleAwsClientFactory                   # STS assume-role client factory
AwsProperties / HttpClientProperties / S3FileIOProperties   # Config from property maps
```
All AWS behavior is configured through catalog/table property maps (credentials,
region, endpoints, SSE, assume-role, etc.) resolved into the `*Properties` types.

## Common Development Tasks

### Using S3FileIO with a catalog
Set `io-impl=org.apache.iceberg.aws.s3.S3FileIO` (plus S3/region properties) on
the catalog; tables then read/write data through S3.

### Using GlueCatalog
Configure `catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`; warehouse and
S3 settings come from properties.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-common` (`implementation`)
- `iceberg-core` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal. Used at runtime by any engine reading
S3/Glue tables; pair it with `iceberg-aws-bundle`, which supplies the relocated
AWS SDK this module compiles against (`compileOnly`).

**Key external libs:** AWS SDK for Java v2 (S3, Glue, STS, DynamoDB, KMS,
LakeFormation — mostly `compileOnly`/runtime via the bundle), Jackson, Caffeine,
Failsafe; Immutables (annotation processor).

## Module Structure Notes

- Depends on `iceberg-core`/`iceberg-api` and the AWS SDK v2; ships as the
  `iceberg-aws` jar (see `iceberg-aws-bundle` for a shaded variant).
- `FileIO` and `Catalog`/`TableOperations` implementations follow the contracts
  defined in api/core.

Build: `./gradlew :iceberg-aws:build` · Test: `./gradlew :iceberg-aws:test`
