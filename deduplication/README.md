# deduplication

Deduplicate records by key within a ten-minute window using a state store to remember previously seen keys.

## Topology

```
input-dedup --> [deduplicate by key] --> output-dedup
```

## Running

```bash
./run.sh
```
