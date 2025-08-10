# materialized-views

Expose a materialized key-value store via a simple REST endpoint for interactive queries.

## Topology

```
input-materialized --> [groupBy/count -> materialized store] --> output-materialized
```

## How to run

```bash
mvn -pl materialized-views -am clean package
java -jar materialized-views/target/materialized-views-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=materialized-views \
  -Dinput.topic=input-materialized \
  -Doutput.topic=output-materialized \
  -Dcounts.store=counts-store \
  -Dvalues.store=values-store
```
