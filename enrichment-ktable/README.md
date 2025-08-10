# Enrichment KTable Pattern

Enrich a stream with table lookups using a `KTable` left join.

## Problem
A fraud detection pipeline must evaluate each card swipe against the latest account
profile.

## Solution
Swipes join to a KTable to fetch the current risk score and decide whether to approve the
transaction.

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

## Generate example data

```bash
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator enrichment-ktable
```
