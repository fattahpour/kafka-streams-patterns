# materialized-views

Expose a materialized key-value store via a simple REST endpoint for interactive queries.

## Topology

```
input-materialized --> [groupBy/count -> materialized store] --> output-materialized
```

## Running

```bash
./run.sh
```
