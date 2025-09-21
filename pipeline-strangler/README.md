# Pipeline Strangler Pattern

Duplicate or divert Kafka traffic between legacy and modern topics while a new pipeline is rolled
out. A feature flag determines whether events flow to the legacy system, the modern system, or both
simultaneously.

## Intent

* `legacy` mode – route all traffic to the legacy topic.
* `modern` mode – route all traffic to the modern topic.
* `dual` mode – duplicate the stream to both topics (default).

The mode is controlled via the `strangler.mode` system property. Metrics count how many events were
sent to each downstream topic.

## Topology

```
(pattern.pipeline-strangler.input)
        |
        v
   [ feature flag router ]
        |             |
        v             v
(pattern.pipeline-strangler.legacy)
(pattern.pipeline-strangler.modern)
```

## Running

```bash
make -C pipeline-strangler build
make -C pipeline-strangler run strangler.mode=modern
```

## Testing

The test suite verifies:

* Dual mode duplicates each event to both topics
* Legacy-only mode routes exclusively to the legacy topic

Execute:

```bash
make -C pipeline-strangler test
```
