# Kafka Streams Patterns

A collection of small Kafka Streams applications demonstrating common stream processing patterns.

## Modules

| Pattern | Module | Status |
|---------|--------|--------|
| Common utilities | [common](common) | Ready |
| Stateless transforms | [stateless-transforms](stateless-transforms) | Ready |
| Branch and route | [branch-route](branch-route) | Ready |
| Rekey/Repartition | [rekey-repartition](rekey-repartition) | Ready |
| KTable enrichment | [enrichment-ktable](enrichment-ktable) | Ready |
| GlobalKTable enrichment | [enrichment-globalktable](enrichment-globalktable) | Ready |
| KStream-KStream join | [join-kstream-kstream](join-kstream-kstream) | Ready |
| KStream-KTable join | [join-kstream-ktable](join-kstream-ktable) | Ready |
| KTable-KTable join | [join-ktable-ktable](join-ktable-ktable) | Ready |
| Tumbling window aggregation | [agg-window-tumbling](agg-window-tumbling) | Ready |
| Hopping window aggregation | [agg-window-hopping](agg-window-hopping) | Ready |
| Session window aggregation | [agg-window-session](agg-window-session) | Ready |
| Aggregate/Reduce/Count | [aggregate-reduce-count](aggregate-reduce-count) | Ready |
| Deduplication | [deduplication](deduplication) | Ready |
| Suppression | [suppression](suppression) | Ready |
| Materialized views | [materialized-views](materialized-views) | Ready |
| Exactly-once outbox | [exactly-once-outbox](exactly-once-outbox) | Ready |
| Retry + DLQ | [retry-dlq](retry-dlq) | Ready |
| Late vs early data | [late-early-data](late-early-data) | Ready |
| Fan-out / Fan-in | [fanout-fanin](fanout-fanin) | Ready |
| Claim check | [claim-check](claim-check) | Ready |
| Event chunking | [event-chunking](event-chunking) | Ready |
| Event gateway connect | [event-gateway-connect](event-gateway-connect) | Ready |
| Idempotent writer/reader | [idempotent-writer-reader](idempotent-writer-reader) | Ready |
| Logical AND multisignal | [logical-and-multisignal](logical-and-multisignal) | Ready |
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
