# iceberg-bundled-guava

## Module Overview

`iceberg-bundled-guava` produces a shaded jar containing Google Guava with its
packages relocated under Iceberg's namespace. Other Iceberg modules depend on
this bundle instead of Guava directly, so Iceberg's Guava version never
conflicts with the (often different) Guava on a query engine's classpath.

## Key Responsibilities

- **Dependency isolation**: relocate `com.google.common.*` to an
  Iceberg-internal shaded package so Iceberg's Guava is insulated from engine
  classpaths.

## Architecture

This module has essentially no source of its own — it is a build artifact. Its
`build.gradle` uses the Gradle Shadow plugin to relocate, roughly:

```
com.google.common.**  ->  org.apache.iceberg.relocated.com.google.common.**
```
and publishes the result as `iceberg-bundled-guava`.

## Common Development Tasks

### Using the bundled Guava
Within Iceberg modules, import Guava classes from the relocated package
(`org.apache.iceberg.relocated.com.google.common...`) — never add a direct
dependency on upstream Guava.

### Bumping the Guava version
Update the Guava version in the version catalog / `build.gradle` and rebuild;
the relocation keeps downstream modules unaffected.

## Dependencies

**Iceberg modules:** none.

**Depended on by:** nearly every Iceberg module (consumed via the `shadow`
configuration: `project(path: ':iceberg-bundled-guava', configuration: 'shadow')`).

**Key external libs:** Google Guava (relocated/shaded).

## Module Structure Notes

- Pure packaging module (shaded jar); no application logic.
- Foundational at build time: most modules consume the relocated Guava it ships.

Build: `./gradlew :iceberg-bundled-guava:build`
