# Enrichment GlobalKTable Pattern

Enrich a stream with table lookups using a `GlobalKTable` left join.

## Scenario

Replicate a small reference dataset to all instances, such as product details, to enrich incoming orders everywhere.

Topology:
```
orders -> LEFTJOIN(products GlobalKTable) -> enriched-orders
```

## How to run

```bash
mvn -pl enrichment-globalktable -am clean package
java -jar enrichment-globalktable/target/enrichment-globalktable-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=enrichment-globalktable-app \
  -Dorders.topic=orders \
  -Dproducts.topic=products \
  -Doutput.topic=enriched-orders
```
