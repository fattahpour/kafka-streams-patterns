# Projection Table with TTL Pattern

Maintain a materialized projection table that honours version ordering and removes stale entries
after a configurable time-to-live (TTL).

## Intent

* Apply idempotent upserts based on a monotonically increasing version number
* Skip out-of-order versions
* Periodically evict projection rows older than the TTL and publish an expiry event

## Topology

```
(pattern.projection-table-ttl.updates)
        |
        v
   [ versioned upsert + TTL sweep ]
        |                        |
        v                        v
(pattern.projection-table-ttl.view)  (pattern.projection-table-ttl.expired)
```

## Configuration

* `projection.ttl.ms` – retention period for projection rows (default `60000` ms)
* Topics: `.updates`, `.view`, `.expired`

## Testing

The tests ensure that newer versions replace older ones and that stale rows are expired when the
wall clock advances beyond the TTL.

```bash
make -C projection-table-ttl test
```
