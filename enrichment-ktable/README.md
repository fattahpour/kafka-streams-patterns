# Enrichment KTable Pattern — Kafka Streams

## Overview
The **Enrichment** pattern in Kafka Streams combines a `KStream` with a `KTable` (or `GlobalKTable`) to add more context to the stream records.
- **KStream**: Event stream (e.g., orders)
- **KTable**: Reference data (e.g., products)
- **Goal**: Join them so each event is enriched with extra data from the table.

---

## What is Enrichment in Kafka Streams?

Enrichment is a **data processing pattern** where an incoming stream of events is **enhanced with additional context** by looking up information from another data source.

Think of it as “**adding missing details**” to each event.

### Everyday Analogy
Imagine you receive a stream of **order IDs with product IDs** from an e-commerce system.  
On their own, these events are not very descriptive:

```
Order: o1, Product: p1
Order: o2, Product: p2
```

You also have a separate dataset that maps **product IDs to product names**:

```
p1 → Product-1
p2 → Product-2
```

By enriching the orders with product names, you make the data more useful:

```
o1 → Product-1
o2 → Product-2
```

This is helpful for:
- Making downstream data human-readable.
- Adding reference data to events for analytics or business logic.
- Reducing the need for downstream services to perform repeated lookups.

### How It Works in Kafka Streams
- **KStream–KTable join**: Matches events in a stream with a lookup table using the **event key**.
- **KStream–GlobalKTable join**: Allows “foreign-key” joins, where the match key can be extracted from the event value.
- The KTable is often backed by a **compacted Kafka topic** containing the latest state for each key.

### Typical Use Cases
- Orders + Product Catalog → Detailed Orders
- User Clicks + User Profiles → Personalized Analytics
- Transactions + Currency Rates → Normalized Financial Data
- IoT Device Readings + Device Metadata → Rich Sensor Data

---

## Common Pitfall
If the keys do not match, the join will return `null` for the table value.

Example (will fail to enrich):
- Orders: key = `o1`, value = `p1`
- Products: key = `p1`, value = `Product-1`
- Join tries to match `o1` with `p1` → no match → `null`.

---

## Solutions
1. **Rekey the KStream** before join:
```java
KStream<String, String> ordersByProductId =
        orders.selectKey((orderId, productId) -> productId);
```

2. **Use GlobalKTable** for foreign-key joins:
```java
orders.leftJoin(productsGlobal,
    (orderId, productId) -> productId,
    (productId, productName) -> orderId + ":" + productName);
```

---

## SerDe Configuration
Always configure SerDes explicitly to avoid `ConfigException`:
```java
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
```

When testing with `TopologyTestDriver`, also specify SerDes in `createInputTopic`/`createOutputTopic`.

---

## Test Example Fix
```java
Properties config = new Properties();
config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-enrichment-ktable");
config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
```

---

## Data Example
Orders (KStream):

| Key | Value |
|-----|-------|
| o1  | p1    |
| o2  | p2    |

Products (KTable):

| Key | Value     |
|-----|-----------|
| p1  | Product-1 |
| p2  | Product-2 |

After rekey to productId:

| Key | Value     |
|-----|-----------|
| p1  | o1        |
| p2  | o2        |

Enriched Output:

| Key | Value          |
|-----|----------------|
| o1  | Product-1      |
| o2  | Product-2      |

---

## Fake Data Generator for Testing (Standalone CLI)
You can generate example data for `orders` and `products` topics using the Kafka CLI:

```bash
# Products topic
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic products \
  --property "parse.key=true" \
  --property "key.separator=:" <<EOF
p1:Product-1
p2:Product-2
p3:Product-3
EOF

# Orders
