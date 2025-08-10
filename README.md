# Kafka Streams Patterns

A collection of small Kafka Streams applications demonstrating common stream processing patterns.

## Modules

- [common](common) – shared utilities
- [stateless-transforms](stateless-transforms) – basic map/filter/flatMap pattern
- [branch-route](branch-route) – branch to multiple topics by predicate
- [rekey-repartition](rekey-repartition) – selectKey with explicit repartitioning
- [enrichment-ktable](enrichment-ktable) – enrich stream via KTable lookup
- [enrichment-globalktable](enrichment-globalktable) – enrich stream via GlobalKTable lookup
- [join-kstream-kstream](join-kstream-kstream) – windowed KStream–KStream join
- [join-kstream-ktable](join-kstream-ktable) – KStream–KTable join
- [join-ktable-ktable](join-ktable-ktable) – KTable–KTable join
- [agg-window-tumbling](agg-window-tumbling) – tumbling window aggregation
- [agg-window-hopping](agg-window-hopping) – hopping window aggregation
- [agg-window-session](agg-window-session) – session window aggregation
- [aggregate-reduce-count](aggregate-reduce-count) – aggregate vs reduce vs count
- [deduplication](deduplication) – deduplicate records by key within a window
 - [suppression](suppression) – emit results only when windows close
 - [materialized-views](materialized-views) – query materialized state via REST
 - [exactly-once-outbox](exactly-once-outbox) – transactional outbox with EOSv2
 - [retry-dlq](retry-dlq) – retry with backoff topics and dead-letter queue
 - [late-early-data](late-early-data) – event-time with grace, early results, and late data handling
- [fanout-fanin](fanout-fanin) – split and merge branches (fan-out/fan-in)

Additional modules for other patterns are planned.

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
| Java      | 17 |
| Maven     | 3.9+ |
| Kafka Streams | 3.7.0 |

## Troubleshooting

- Serialization errors – check Serdes configuration
- Repartitioning gotchas – ensure keys are set before grouping
- EOS pitfalls – use `processing.guarantee=exactly_once_v2`

## License

Apache-2.0
