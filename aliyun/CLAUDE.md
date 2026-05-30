# iceberg-aliyun

## Module Overview

`iceberg-aliyun` provides Alibaba Cloud integration for Iceberg, primarily an
Iceberg `FileIO` over Alibaba Cloud Object Storage Service (OSS). It builds on
the Aliyun OSS Java SDK.

## Key Responsibilities

- **Storage**: `OSSFileIO` — Iceberg `FileIO` over Alibaba Cloud OSS.
- **Configuration**: OSS credentials/endpoint via property maps.
- **Test support**: an embedded OSS mock for unit testing.

## Architecture

### Storage — OSS (`org.apache.iceberg.aliyun.oss`)

```
OSSFileIO          # FileIO over Aliyun OSS
OSSURI             # Parse/represent oss:// locations (bucket/key)
OSSInputFile / OSSOutputFile / OSSInputStream / OSSOutputStream
OSSFileIOProperties# OSS-specific configuration
oss/mock/          # AliyunOSSMockApp + local store/controller for tests
```

### Configuration (`org.apache.iceberg.aliyun`)

```
AliyunProperties                       # Endpoint, access key id/secret, etc.
AliyunClientFactory / AliyunClientFactories   # Build OSS clients
```
OSS behavior is configured through property maps resolved into `AliyunProperties`
/ `OSSFileIOProperties`.

## Common Development Tasks

### Using OSSFileIO
Set `io-impl=org.apache.iceberg.aliyun.oss.OSSFileIO` on the catalog with the
relevant Aliyun properties; tables then read/write data on OSS via `oss://`
locations.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-core` (`implementation`)
- `iceberg-common` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** nothing internal (leaf cloud module); used at runtime by
engines reading OSS tables. (No dedicated `-bundle` module.)

**Key external libs:** Aliyun OSS SDK (`compileOnly`), Aliyun credentials/tea;
JAXB (`compileOnly`).

## Module Structure Notes

- Depends on `iceberg-core`/`iceberg-api` and the Aliyun OSS SDK.
- Implements the `FileIO`/`InputFile`/`OutputFile` contracts from api/core.

Build: `./gradlew :iceberg-aliyun:build` · Test: `./gradlew :iceberg-aliyun:test`
