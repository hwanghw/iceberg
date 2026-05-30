# iceberg-orc

## Module Overview

`iceberg-orc` integrates Apache ORC with Iceberg. It reads and writes ORC files
mapped to Iceberg schemas, supports both row-based and vectorized (batch) reads,
applies predicate pushdown, and derives per-file column statistics for scan
planning. ORC is one of Iceberg's supported columnar data file formats.

## Key Responsibilities

- **Read/write**: build ORC readers and writers bound to an Iceberg schema.
- **Schema mapping**: convert between Iceberg and ORC types using field IDs.
- **Row & batch reads**: row-based and vectorized `VectorizedRowBatch` decoding.
- **Metrics**: derive Iceberg `Metrics` from ORC file statistics.

## Architecture

### Entry point & IO (`org.apache.iceberg.orc`)

```
ORC                # Fluent builder: ORC.read(file)/write(file)...
OrcIterable        # Iterable over decoded rows
OrcRowReader / OrcValueReader(s)    # Row-based value decoding
OrcBatchReader / VectorizedRowBatchIterator   # Vectorized batch decoding
OrcRowWriter / OrcValueWriter(s)    # Value encoding for writes
ORCSchemaUtil      # Iceberg <-> ORC schema conversion (field-ID based)
OrcMetrics         # File-statistics → Iceberg Metrics
```

`ORC` is the canonical entry point; a read/write is configured via its builder
with the Iceberg schema and a function that creates the appropriate value
reader/writer for the target row or batch representation. Field IDs are stored
in ORC type attributes so schema evolution is preserved.

## Common Development Tasks

### Reading ORC into a row type
```java
CloseableIterable<T> rows = ORC.read(inputFile)
    .project(schema)
    .createReaderFunc(fileSchema -> ...valueReader...)
    .filter(expression)
    .build();
```

### Writing a data file
Use `ORC.write(outputFile).schema(schema).createWriterFunc(...).build()`, write
rows, and close to obtain a `DataFile` with collected metrics.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-common` (`implementation`)
- `iceberg-core` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** `iceberg-data` (format dispatch) and the engine modules
(`mr`, `spark`, `flink`).

**Key external libs:** Apache ORC.

## Module Structure Notes

- Depends on `iceberg-core`/`iceberg-api` and the Apache ORC library; bridges the
  external format to Iceberg's schema/type system and `FileIO`.
- Mapping is by **field ID** via ORC type attributes, preserving evolution.
- Batch reading feeds vectorized engine read paths.

Build: `./gradlew :iceberg-orc:build` · Test: `./gradlew :iceberg-orc:test`
