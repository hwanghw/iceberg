# iceberg-arrow

## Module Overview

`iceberg-arrow` provides vectorized, columnar reads of Iceberg data as Apache
Arrow vectors. It converts Iceberg/Parquet column data into Arrow's in-memory
columnar format, enabling efficient batch processing and zero-copy interchange
with Arrow-based consumers.

## Key Responsibilities

- **Vectorized reads**: read Iceberg data files into Arrow `VectorSchemaRoot`s.
- **Schema mapping**: convert Iceberg schemas to Arrow schemas.
- **Columnar decoding**: populate Arrow `FieldVector`s from Parquet column data.
- **Memory management**: manage Arrow off-heap allocation.

## Architecture

### Arrow integration (`org.apache.iceberg.arrow`)

```
ArrowSchemaUtil    # Iceberg Schema -> Arrow Schema conversion
ArrowAllocation    # Arrow BufferAllocator setup / memory management
```

### Vectorized readers (`org.apache.iceberg.arrow.vectorized`)

```
ArrowReader                 # Reads a table scan into Arrow batches
VectorizedArrowReader       # Reads one column into an Arrow vector
VectorizedReader            # Vectorized reader interface
VectorHolder                # Holds an Arrow vector + null/dictionary info
ArrowVectorAccessor         # Typed access to values in an Arrow vector
```

The vectorized readers build on `iceberg-parquet`'s columnar reader: Parquet
column chunks are decoded directly into Arrow `FieldVector`s (with dictionary
and null handling) rather than into per-row objects.

## Common Development Tasks

### Reading a table as Arrow batches
Construct an `ArrowReader` over a `TableScan` and iterate the resulting columnar
batches; remember to close vectors/allocators to release off-heap memory.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-core` (`implementation`)
- `iceberg-parquet` (`implementation` — vectorized reads build on it)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** the Spark integration's vectorized read path.

**Key external libs:** Apache Arrow; Netty buffer (`runtimeOnly`).

## Module Structure Notes

- Depends on `iceberg-core`/`iceberg-api`, `iceberg-parquet`, and Apache Arrow.
- Read-oriented: it produces Arrow vectors; it is not a general writer.
- Underpins vectorized read paths in engine integrations (e.g. Spark).
- Arrow uses off-heap memory — be careful to close vectors/allocators.

Build: `./gradlew :iceberg-arrow:build` · Test: `./gradlew :iceberg-arrow:test`
