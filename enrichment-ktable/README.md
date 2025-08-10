# Enrichment KTable Pattern

Enrich a stream with table lookups using a `KTable` left join.

## Scenario

Join events with partitioned reference data, like matching orders with customer records stored in a co-partitioned table.

Topology:
```
orders -> LEFTJOIN(products KTable) -> enriched-orders
```

## How to run

```bash
mvn -pl enrichment-ktable -am clean package
java -jar enrichment-ktable/target/enrichment-ktable-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=enrichment-ktable-app \
  -Dorders.topic=orders \
  -Dproducts.topic=products \
  -Doutput.topic=enriched-orders
```
