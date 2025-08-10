# Enrichment GlobalKTable Pattern

Enrich a stream with table lookups using a `GlobalKTable` left join.

## Problem
A shipping network needs package-scan events enriched with depot information that rarely
changes.

## Solution
The depot table is replicated as a GlobalKTable so each instance can join scans with depot
data locally.

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

## Generate example data

```bash
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator enrichment-globalktable
```
