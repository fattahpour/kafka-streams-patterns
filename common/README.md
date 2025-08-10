# Common Utilities

Shared classes and helpers used across the Kafka Streams pattern modules.

## Scenario

Provide reusable serializers and utilities for stream applications so each module can focus on its specific pattern.

## Build and Install

```bash
mvn -pl common -am clean install
```

This compiles the module and installs it to your local Maven repository so other modules can depend on it.

## Run Tests

```bash
mvn -pl common -am test
```

The common module does not produce a runnable JAR by itself. It is consumed by the other example modules in this repository.
