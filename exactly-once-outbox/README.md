# Exactly-Once Outbox Pattern

Demonstrates using Kafka Streams with EOSv2 to atomically write to a processed topic and an outbox topic.

## Scenario

Generate events for external systems while guaranteeing once-only processing, for example emitting an order confirmation and a side-effect event.

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
