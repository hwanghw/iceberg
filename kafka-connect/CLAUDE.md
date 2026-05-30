# iceberg-kafka-connect

## Module Overview

`iceberg-kafka-connect` is a Kafka Connect **sink connector** that streams
records from Kafka topics into Iceberg tables. It coordinates commits across
distributed sink tasks using a Kafka control topic so that data written from
multiple workers is committed to Iceberg consistently (aligned with Kafka
offsets).

## Key Responsibilities

- **Sink connector/task**: receive Kafka records and write them as Iceberg data
  files.
- **Commit coordination**: a control-topic protocol that synchronizes a commit
  across all workers (coordinator/worker roles).
- **Data conversion**: map Kafka Connect records to Iceberg rows, with optional
  schema handling and multi-table routing.

## Architecture / Subprojects

Defined in `settings.gradle` (built when `kafkaVersions` includes `3`):

```
kafka-connect/
   ├── kafka-connect/            # iceberg-kafka-connect: the sink connector + commit protocol
   ├── kafka-connect-events/     # iceberg-kafka-connect-events: Avro event types for the protocol
   ├── kafka-connect-transforms/ # iceberg-kafka-connect-transforms: SMTs (record transforms)
   └── kafka-connect-runtime/    # iceberg-kafka-connect-runtime: packaged/distributable connector
```

### Connector & coordination (`org.apache.iceberg.connect`, `connect.channel`)

```
connect/                 # IcebergSinkConnector, IcebergSinkTask, IcebergSinkConfig
connect/channel/         # Coordinator, Worker, Channel, CommitterImpl —
                         #   control-topic-based commit coordination
connect/data/            # IcebergWriter, record-to-row conversion, multi-table routing
```

### Events (`org.apache.iceberg.connect.events`)
Avro-serialized event payloads (commit request/response, data-written, etc.)
exchanged over the control topic to drive the distributed commit protocol.

### Transforms (`org.apache.iceberg.connect.transforms`)
Kafka Connect Single Message Transforms (SMTs) for pre-processing records.

### Commit protocol (high level)
A coordinator periodically starts a commit round on the control topic; workers
flush their in-progress Iceberg data files and report them; the coordinator
gathers the results and performs a single Iceberg commit, tying the table commit
to a consistent set of Kafka offsets.

## Common Development Tasks

### Configuring the sink
Deploy the connector with `connector.class=org.apache.iceberg.connect.IcebergSinkConnector`
and configure the target catalog, table(s)/routing, and control topic.

## Dependencies

**`iceberg-kafka-connect` (main connector):**
- `iceberg-api` (`api`), `iceberg-kafka-connect-events` (`api`)
- `iceberg-common`, `iceberg-core`, `iceberg-data` (`implementation`)
- `iceberg-bundled-guava` (`implementation`)
- `iceberg-hive-metastore`, Hadoop common (`compileOnly`)

**`iceberg-kafka-connect-events`:** `iceberg-api` (`api`); `iceberg-core`,
`iceberg-common`, `iceberg-bundled-guava` (`implementation`) + Avro.

**`iceberg-kafka-connect-transforms`:** Avro (SMTs; no core Iceberg dependency).

**`iceberg-kafka-connect-runtime`:** depends on `iceberg-kafka-connect` and
packages the deployable connector archive (with catalog/FileIO modules added as
needed).

**Depended on by:** nothing internal; the `-runtime` archive is the deployment
artifact.

**Key external libs:** Kafka Connect API, Avro.

## Module Structure Notes

- Multi-subproject module; the `-runtime` subproject produces the deployable
  connector archive.
- Depends on `iceberg-core` + catalog/FileIO modules; integrates with the Kafka
  Connect framework.

Build: `./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:build`
