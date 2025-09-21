# Kafka Streams Patterns

A collection of small Kafka Streams applications demonstrating common stream processing patterns.

## Modules

| Module | Description |
|--------|-------------|
| [common](common) | Shared SerDes, header utilities, and test helpers used by every pattern. |
| [stateless-transforms](stateless-transforms) | Demonstrates simple map/filter/branch processing. |
| [branch-route](branch-route) | Branch events into multiple topics based on predicates. |
| [rekey-repartition](rekey-repartition) | Change record keys and trigger repartitioning safely. |
| [enrichment-ktable](enrichment-ktable) | Enrich a stream from a co-partitioned KTable lookup. |
| [enrichment-globalktable](enrichment-globalktable) | Enrich using a GlobalKTable for fan-out reads. |
| [join-kstream-kstream](join-kstream-kstream) | Join two streams with windowed semantics. |
| [join-kstream-ktable](join-kstream-ktable) | Join a stream to a table for latest-state lookups. |
| [join-ktable-ktable](join-ktable-ktable) | Illustrates table-table joins for merged state. |
| [agg-window-tumbling](agg-window-tumbling) | Tumbling window aggregation example. |
| [agg-window-hopping](agg-window-hopping) | Hopping window aggregation example. |
| [agg-window-session](agg-window-session) | Session window aggregation example. |
| [aggregate-reduce-count](aggregate-reduce-count) | Compare `aggregate`, `reduce`, and `count`. |
| [deduplication](deduplication) | Idempotent stream deduplication with state store. |
| [suppression](suppression) | Suppress intermediate results until windows close. |
| [materialized-views](materialized-views) | Build read models backed by interactive queries. |
| [exactly-once-outbox](exactly-once-outbox) | Exactly-once pattern bridging transactional outbox. |
| [retry-dlq](retry-dlq) | Retry with exponential backoff and dead letter queue. |
| [late-early-data](late-early-data) | Route early vs. late arrivals based on timestamps. |
| [fanout-fanin](fanout-fanin) | Broadcast results and re-aggregate fan-in responses. |
| [claim-check](claim-check) | Offload large payloads via the claim-check pattern. |
| [event-chunking](event-chunking) | Chunk large events and reassemble on the consumer side. |
| [event-gateway-connect](event-gateway-connect) | Gateway processor that annotates headers for Kafka Connect sinks. |
| [idempotent-writer-reader](idempotent-writer-reader) | Demonstrates idempotent producers and readers using shared state. |
| [logical-and-multisignal](logical-and-multisignal) | Correlate multiple heterogeneous signals before emission. |
| [wallclock-timers](wallclock-timers) | Fire timers at wall-clock intervals using punctuators. |
| [event-splitter](event-splitter) | Decompose composite events into lineage-tracked child events. |
| [event-collaboration](event-collaboration) | Collaborate heterogeneous streams with lateness tolerance. |
| [cqrs-projections](cqrs-projections) | Apply CQRS commands and emit projection snapshots. |
| [saga-orchestration](saga-orchestration) | Orchestrate an order saga with compensating actions. |
| [geo-replication-notes](geo-replication-notes) | Documentation and configs for multi-region replication with MirrorMaker 2. |
| [pipeline-strangler](pipeline-strangler) | Feature-flag router that diverts traffic between legacy and modern topics. |
| [content-filter](content-filter) | Early-drop filter that rejects banned or oversized payloads. |
| [projection-table-ttl](projection-table-ttl) | Versioned materialized view with TTL-based eviction. |
## Local Development

```bash
docker compose up -d
mvn -pl <module> -am clean package
java -jar <module>/target/*.jar
```

### Generating sample data

The `common` module provides a helper to seed topics with example records for any pattern
module:

```bash
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator [module ...]
```

Run the generator without arguments to seed every module, or pass one or more
module names (for example, `branch-route` or `aggregate-reduce-count`) to seed
only those modules' input topics with fake data.

## Version Matrix

| Component | Version |
|-----------|---------|
| Java      | 25 |
| Maven     | 3.9+ |
| Kafka Streams | 3.7.0 |

## Troubleshooting

- Serialization errors – check Serdes configuration
- Repartitioning gotchas – ensure keys are set before grouping
- EOS pitfalls – use `processing.guarantee=exactly_once_v2`

## License

Apache-2.0
