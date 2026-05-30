# iceberg-parquet

## Module Overview

`iceberg-parquet` integrates Apache Parquet with Iceberg. It reads and writes
Parquet files mapped to Iceberg schemas (by field ID), applies column projection
and predicate pushdown, and collects per-file column statistics used by Iceberg
scan planning. Parquet is Iceberg's most commonly used data file format.

## Key Responsibilities

- **Read/write**: build Parquet readers and writers bound to an Iceberg schema.
- **Schema mapping**: convert between Iceberg and Parquet types using field IDs.
- **Pushdown & projection**: translate Iceberg expressions to Parquet filters.
- **Metrics**: derive Iceberg `Metrics` (bounds, null/value counts) from footers.
- **Vectorized reads**: support columnar reads (used by the Arrow/Spark paths).

## Architecture

### Entry point & IO (`org.apache.iceberg.parquet`)

```
Parquet                 # Fluent builder: Parquet.read(file)/write(file)...
ParquetReader           # Row-based reader
VectorizedParquetReader # Columnar/batch reader
ParquetWriter           # Writer producing DataFiles
ParquetValueReaders / ParquetValueWriters   # Per-type value codecs
ParquetSchemaUtil       # Iceberg <-> Parquet schema conversion (field-ID based)
ParquetTypeVisitor      # Visitor framework over Parquet message types
ParquetUtil             # Footer-derived column statistics / Metrics
```

The `Parquet` class is the canonical entry point; callers configure a read/write
via its builder, supplying the Iceberg schema and a function that creates the
value reader/writer for the target row representation.

## Common Development Tasks

### Reading Parquet into a row type
```java
CloseableIterable<T> rows = Parquet.read(inputFile)
    .project(schema)
    .createReaderFunc(fileSchema -> ...valueReader...)
    .filter(expression)
    .build();
```

### Writing a data file
Use `Parquet.write(outputFile).schema(schema).createWriterFunc(...).build()`,
write rows, then close to obtain a `DataFile` with collected metrics.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-core` (`implementation`)
- `iceberg-common` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** `iceberg-data`, `iceberg-arrow` (vectorized path), and the
engine modules (`mr`, `spark`, `flink`).

**Key external libs:** Apache Parquet; Avro (`compileOnly`).

## Module Structure Notes

- Depends on `iceberg-core`/`iceberg-api` and the Apache Parquet library; bridges
  the external format to Iceberg's schema/type system and `FileIO`.
- Mapping is by **field ID**, so projection/evolution work even when column
  order or names change.
- The vectorized reader underpins `iceberg-arrow` and engine columnar reads.

Build: `./gradlew :iceberg-parquet:build` · Test: `./gradlew :iceberg-parquet:test`
