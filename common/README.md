# Common Utilities

Shared classes and helpers used across the Kafka Streams pattern modules.

## Problem
Multiple stream-processing services share serialization and configuration logic, leading to
duplicated boilerplate.

## Solution
This module centralizes those utilities so each example can focus solely on its pattern.

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
