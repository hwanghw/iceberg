# iceberg-open-api

## Module Overview

`iceberg-open-api` is the home of the **Iceberg REST Catalog specification** and
its compatibility test kit. It is not a normal library module: its primary
artifact is the OpenAPI document that defines the REST Catalog protocol, plus
tooling and a test server used to validate REST catalog implementations against
that spec.

## Key Responsibilities

- **Protocol spec**: `rest-catalog-open-api.yaml` is the authoritative
  definition of the Iceberg REST Catalog HTTP API (the source of truth).
- **Generated bindings**: a Python model generated from the YAML.
- **Compatibility kit (RCK)**: a JUnit suite + embedded REST server to verify any
  REST catalog server conforms to the spec.

## Architecture

### Layout

```
rest-catalog-open-api.yaml   # The REST Catalog OpenAPI spec (source of truth)
rest-catalog-open-api.py     # Python model generated from the spec
requirements.txt / Makefile  # Tooling to (re)generate code from the spec
src/testFixtures/...         # RESTServerExtension, RCKUtils — embedded test server harness
src/test/...                 # RESTCompatibilityKit{Catalog,ViewCatalog}Tests + suite
```

### Compatibility Kit (`org.apache.iceberg.rest`)

```
RESTServerExtension                       # Spins up a REST catalog server for tests
RCKUtils                                  # REST compatibility-kit helpers
RESTCompatibilityKitCatalogTests          # Conformance tests for table operations
RESTCompatibilityKitViewCatalogTests      # Conformance tests for view operations
RESTCompatibilityKitSuite                 # Aggregated suite
```
The REST **client** lives in `iceberg-core` (`org.apache.iceberg.rest`); this
module owns the spec and the conformance tests/server fixtures.

## Common Development Tasks

### Changing the REST protocol
Edit `rest-catalog-open-api.yaml` first (it is the contract), then regenerate the
Python model (see the `Makefile`) and update the core client + RCK tests to match.

### Validating a REST catalog server
Run the RCK suite against the server to check spec conformance.

## Dependencies

**Iceberg modules (test / test-fixtures only — no production deps):**
- `iceberg-api`, `iceberg-core` (`testImplementation` / `testFixturesImplementation`)
- `iceberg-aws`, `iceberg-gcp`, `iceberg-azure`, `iceberg-bigquery`
  (`testFixturesImplementation` — to exercise cloud-backed REST catalogs)
- `iceberg-aws-bundle`, `iceberg-gcp-bundle`, `iceberg-azure-bundle`
  (`testImplementation` / `testFixturesRuntimeOnly`)

**Depended on by:** nothing internal — it is the spec + conformance kit. The REST
**client** lives in `iceberg-core` (`org.apache.iceberg.rest`).

**Key external libs:** OpenAPI tooling (codegen via the `Makefile`/Python),
JUnit; a test REST server harness.

## Module Structure Notes

- Spec/tooling/test module — minimal production Java; the runtime client is in
  `iceberg-core`.
- Keep `rest-catalog-open-api.yaml` and the generated model in sync.

Build: `./gradlew :iceberg-open-api:build` (and `Makefile` targets for codegen)
