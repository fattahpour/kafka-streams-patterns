# Enrichment — `KStream` ⟵ `GlobalKTable`

Enrich a high-volume event stream with small, reference data replicated locally on every Streams instance using **GlobalKTable**. This avoids cross-partition lookups and is ideal when the lookup dataset comfortably fits on each node.

## What this example shows
- Read purchases as a `KStream` (key = purchaseId, value contains `productId`, `qty`)
- Materialize products as a **GlobalKTable** (key = `productId`, value = `productName`)
- `leftJoin` the stream with the GlobalKTable using a **key-mapper** that extracts `productId` from the stream record
- Emit `productName:qty`, falling back to `unknown:qty` when no match exists

> Why GlobalKTable? Each app instance holds a **full copy** of the table’s changelog topic, enabling constant-time local lookups without co-partitioning. Best for small/medium, slowly changing reference data.

## Topology (high level)
```
 purchases (KStream) --(value.productId)--> [leftJoin] <-- (Global, replicated) products (GlobalKTable)
                                               |
                                               v
                                     enriched (KStream)
```

## Topics
- `products` — changelog of product reference data (`key=productId`, `value=productName`)
- `purchases` — incoming transactions (`value` contains `productId`, `qty`)
- `enriched-purchases` — output of the join

## Build
From repo root:
```bash
mvn -pl enrichment-globalktable -am clean package
```

## Run
```bash
java -jar enrichment-globalktable/target/enrichment-globalktable-*.jar
```

## Seed sample data

### Option A) Kafka CLI (local Kafka)
```bash
# products (GlobalKTable source)
kafka-console-producer.sh --topic products --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=":"
p1:apple
p2:banana

# purchases (KStream source)
kafka-console-producer.sh --topic purchases --bootstrap-server localhost:9092
{"purchaseId":"x1","productId":"p1","qty":5}
{"purchaseId":"x2","productId":"p2","qty":3}
{"purchaseId":"x3","productId":"p9","qty":1}
```

### Option B) Confluent CLI
```bash
confluent kafka topic produce products --parse-key --delimiter ":"
p1:apple
p2:banana
```

## Expected output
```bash
kafka-console-consumer.sh --topic enriched-purchases --from-beginning --bootstrap-server localhost:9092
```
Output:
```
KeyValue(p1, apple:5)
KeyValue(p2, banana:3)
KeyValue(p9, unknown:1)
```

## Key code snippet (essentials)
```java
final StreamsBuilder b = new StreamsBuilder();

final KStream<String, Purchase> purchases =
    b.stream("purchases", Consumed.with(Serdes.String(), purchaseSerde));

final GlobalKTable<String, String> products =
    b.globalTable("products", Consumed.with(Serdes.String(), Serdes.String()));

final KStream<String, String> enriched =
    purchases.leftJoin(
        products,
        (purchaseId, p) -> p.getProductId(),
        (p, name) -> (name == null ? "unknown" : name) + ":" + p.getQty()
    );

enriched.to("enriched-purchases", Produced.with(Serdes.String(), Serdes.String()));
```


## When to use `GlobalKTable` vs `KTable`
- **GlobalKTable**: small/medium reference data, no co-partitioning needed.
- **KTable**: large datasets with co-partitioning.
