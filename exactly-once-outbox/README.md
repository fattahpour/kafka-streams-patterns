# Exactly-Once Outbox Pattern

Demonstrates using Kafka Streams with EOSv2 to atomically write to a processed topic and an outbox topic.

## Problem
An online store needs to publish order updates and send confirmation emails exactly once.

## Solution
The outbox pattern writes the processed order and notification event atomically,
guaranteeing each order is acted on once.

Topology:
```
orders -> MAP(toUpperCase) -> [processed-orders, orders-outbox]
```

## How to run

```bash
mvn -pl exactly-once-outbox -am clean package
java -jar exactly-once-outbox/target/exactly-once-outbox-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=exactly-once-outbox-app \
  -Dinput.topic=orders \
  -Dprocessed.topic=processed-orders \
  -Doutbox.topic=orders-outbox
```

## Generate example data

```bash
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator exactly-once-outbox
```
