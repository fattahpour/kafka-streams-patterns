# Idempotent Writer/Reader Pattern

Combine per-event deduplication with EOSv2 guarantees to deliver each event exactly once, even
across process restarts.

## Intent

The writer stage filters duplicate `eventId`s using a persistent RocksDB state store before
publishing to a writer topic. A downstream reader stage materialises its own compacted store to
suppress re-delivery after restarts, emitting only the first observation of each event.

## Topology

```
(pattern.idempotent-writer-reader.in)
        |
        v
    [ writer dedupe ] --valid--> (pattern.idempotent-writer-reader.writer)
        |                                 |
        +--> (missing id) --> (pattern.idempotent-writer-reader.dlq)
                                          |
                                          v
                              [ reader dedupe / replay guard ]
                                          |
                                          v
                           (pattern.idempotent-writer-reader.out)
```

## Configuration

* Topics: `.in`, `.writer`, `.out`, `.dlq`
* Headers: `correlation-id`, `causation-id`
* Metrics: writer processed/emitted counts, reader emitted count, DLQ count
* Processing guarantee: `exactly_once_v2`

Sample properties are available in `src/main/resources/application.properties`.

## Running

```bash
make -C idempotent-writer-reader build
make -C idempotent-writer-reader run
```

## Testing

Test coverage ensures:

* Writer only forwards the first instance of a duplicate event
* Reader state survives process restarts using the on-disk RocksDB store
* Invalid payloads (missing event IDs) are sent to the DLQ

Execute all tests via:

```bash
make -C idempotent-writer-reader test
```
