# iceberg-data

## Module Overview

`iceberg-data` provides an **engine-independent** way to read and write Iceberg
table rows using a generic in-memory record model. It ties together the core
table format with the file-format modules (Avro/Parquet/ORC) so you can read and
write tables without Spark, Flink, or Hive. It is widely used in tests and by
tools that need plain Java access to table data.

## Key Responsibilities

- **Generic record model**: `Record`/`GenericRecord` representing a row by schema.
- **Reading rows**: scan a table and iterate decoded records, applying deletes.
- **Writing rows**: produce data (and delete) files via appender/writer factories.
- **Format bridging**: dispatch to Avro, Parquet, or ORC readers/writers.

## Architecture

### Generic record model & IO (`org.apache.iceberg.data`)

```
Record / GenericRecord          # Schema-described row, field access by name/pos
IcebergGenerics                 # Fluent entry point to read records from a Table
GenericReader                   # Plans the scan and decodes matching files
GenericAppenderFactory          # Builds FileAppenders for data files
GenericFileWriterFactory        # Builds data + delete file writers
```

### Format dispatch (`org.apache.iceberg.data.{avro,parquet,orc}`)

```
data/avro      # Generic <-> Avro readers/writers
data/parquet   # Generic <-> Parquet readers/writers (via iceberg-parquet)
data/orc       # Generic <-> ORC readers/writers (via iceberg-orc)
```
The factories pick the right format implementation based on each file's
`FileFormat`, so callers work in terms of generic `Record`s regardless of how
data is physically stored.

## Common Development Tasks

### Reading a table generically
```java
CloseableIterable<Record> rows =
    IcebergGenerics.read(table).where(Expressions.equal("id", 1)).build();
```

### Writing data files
Use `GenericAppenderFactory` / `GenericFileWriterFactory` to create appenders,
write `Record`s, then commit the resulting `DataFile`s through a table append.

## Dependencies

**Iceberg modules:**
- `iceberg-api` (`api`)
- `iceberg-core` (`implementation`)
- `iceberg-parquet`, `iceberg-orc` (`compileOnly` — format dispatch is optional
  at compile time; provide them at runtime to read/write those formats)
- `iceberg-bundled-guava` (`implementation`)

**Depended on by:** the engine integrations (`mr`, `spark`, `flink`,
`kafka-connect`) and tools needing engine-free row access.

**Key external libs:** Avro (`compileOnly`); Parquet/ORC come via the format
modules.

## Module Structure Notes

- Depends on `iceberg-core` plus the format modules (`iceberg-parquet`,
  `iceberg-orc`); Avro support comes through core.
- This is the "no-engine" reference IO path — engine integrations have their own
  optimized (often vectorized) readers/writers instead of `Record`.

Build: `./gradlew :iceberg-data:build` · Test: `./gradlew :iceberg-data:test`
