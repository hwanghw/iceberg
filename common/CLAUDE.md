# iceberg-common

## Module Overview

`iceberg-common` is a tiny utility module providing dynamic reflection helpers.
These let Iceberg load and bind to optional or version-specific dependencies at
runtime without a hard compile-time dependency — essential for a project that
supports many interchangeable engines, catalogs, and storage backends.

## Key Responsibilities

- **Dynamic reflection**: build and invoke classes, constructors, fields, and
  methods that may not be present at compile time.
- **Optional-dependency loading**: gracefully pick an implementation based on
  what is available on the classpath.

## Architecture

### Reflection utilities (`org.apache.iceberg.common`)

```
DynClasses        # Resolve a class by trying multiple candidate names
DynConstructors   # Find/bind a constructor and build instances reflectively
DynMethods        # Find/bind static or instance methods (with fallbacks)
DynFields         # Find/bind static or instance fields
```

Each `Dyn*` class uses a fluent **builder** that tries candidate
names/signatures in order and either binds the first match or fails with a clear
error. This is how, for example, code can target a method that exists only in
some Hadoop or engine versions.

## Common Development Tasks

### Binding to an optional method
```java
DynMethods.builder("someMethod")
    .impl("com.optional.Target", String.class)
    .orNoop()              // tolerate absence
    .build();
```

### Loading one of several possible classes
```java
Class<?> impl = DynClasses.builder()
    .impl("preferred.Impl")
    .impl("fallback.Impl")
    .buildChecked();
```

## Dependencies

**Iceberg modules:**
- `iceberg-bundled-guava` (relocated Guava) — its only internal dependency.

**Depended on by:** `iceberg-core` and most cloud/catalog/engine modules (which
use the `Dyn*` helpers to bind optional dependencies).

**Key external libs:** none.

## Module Structure Notes

- One of the lowest-level modules: depended on by `iceberg-core` and most other
  modules; depends on essentially nothing itself.
- Keep it dependency-free and stable — many modules rely on these helpers.

Build: `./gradlew :iceberg-common:build` · Test: `./gradlew :iceberg-common:test`
