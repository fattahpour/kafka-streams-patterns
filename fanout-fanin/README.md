# Fanout-Fanin Pattern

The **fanout-fanin** module demonstrates how a single Kafka topic can be split into
parallel processing paths and recombined into a consolidated output stream. It
illustrates the fan-out/fan-in pattern with simple arithmetic logic while also offering
a production-inspired code challenge for deeper practice.

## Purpose and context
The module shows how to branch a Kafka Streams topology, transform each branch, and
merge the results. This pattern is useful when different business rules must be applied
to the same event in parallel before emitting a unified result.

Typical use cases include:
- Applying different enrichment or scoring models to a shared input event.
- Running compute-intensive analysis (e.g., image or text processing) in parallel and
  merging the results.
- Routing records to specialised processing pipelines while preserving a unified output
  topic for downstream consumers.

## Demonstration scenario
An image-processing service must run multiple analyses on each photo and combine the
results. The demo topology branches uploads into separate streams for face detection and
object labelling, then merges the outputs into a single metadata record.

### TopologyBuilder responsibilities
`TopologyBuilder` wires the streaming topology. It reads configuration from system
properties, applies branching logic, transforms branch outputs, and merges the results.

Key responsibilities:
1. Resolve input and output topic names via the `input.topic` and `output.topic` system
   properties (falling back to `fanout-input` and `fanout-output`).
2. Create a `StreamsBuilder` and subscribe to the input topic with `String` serdes.
3. Branch the `KStream` into two streams:
   - **Even branch** – records whose value parses to an even integer.
   - **Odd branch** – all other records (including unparsable values).
4. Transform branch values independently: even values are doubled, odd values are
   tripled (unparsable values are passed through unchanged).
5. Merge the two streams and publish the result to the configured output topic.

### ASCII topology graph
```
          ┌────────────────────┐
          │  fanout-input topic│
          └─────────┬──────────┘
                    │
             StreamsBuilder
                    │
                 branch()
        ┌──────────┴───────────┐
        │                      │
 ┌──────▼──────┐        ┌──────▼──────┐
 │ even branch │        │  odd branch │
 │  (v % 2=0)  │        │   fallback  │
 └──────┬──────┘        └──────┬──────┘
        │ mapValues(v*2)       │ mapValues(v*3 or pass-through)
        └──────────┬───────────┘
                   │
                 merge()
                   │
          fanout-output topic
```

### Data flow walkthrough
1. **Consumption** – Records are consumed from the input topic as `(String key,
   String value)` pairs.
2. **Branching** – `branch()` attempts to parse the record value as an integer. When the
   value is an even number, the record moves to the _even branch_. All other values,
   including non-numeric payloads, flow into the _odd branch_.
3. **Transformation** – Each branch applies its own `mapValues` transformation. The even
   branch doubles the numeric value; the odd branch triples numeric values while
   preserving non-numeric payloads.
4. **Merge** – Both streams are merged with `merge()`. Ordering is not guaranteed; Kafka
   Streams interleaves records as they become available.
5. **Publication** – The merged stream is written to the output topic using string serdes.

### Configuration surface
- `input.topic` – optional system property overriding the input topic (default:
  `fanout-input`).
- `output.topic` – optional system property overriding the output topic (default:
  `fanout-output`).
- Global Kafka Streams configurations (bootstrap servers, application id, etc.) are
  supplied via the standard Kafka Streams properties when the application starts.

### Operational considerations
- **Error handling** – Non-numeric payloads are routed to the odd branch and emitted
  unchanged. This keeps the pipeline resilient to malformed data at the cost of mixing
  processed and raw values on the output topic.
- **Scaling** – Since branching happens in-memory within a task, scaling the application
  horizontally increases throughput for both branches simultaneously.
- **Extensibility** – Additional branches or transformations can be added by extending
  the branching predicate array and applying new per-branch logic prior to the merge.

## How to run the example
```
mvn -pl fanout-fanin -am clean package
java -jar fanout-fanin/target/fanout-fanin-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=fanout-fanin-app \
  -Dinput.topic=fanout-input \
  -Doutput.topic=fanout-output
```

### Generate example data
```
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator fanout-fanin
```

The generated events will illustrate how values are routed and transformed across the
fan-out/fan-in topology.

## Code challenge: Real-time order enrichment
If you want to apply the concepts in a realistic context, tackle the following Velocity
Retail brief. It describes a branching + merging enrichment workflow that you can build
on top of the module’s scaffolding. The challenge is designed for workshops or interview
exercises and typically takes 2–4 hours to implement.

### Scenario
You have joined the **Velocity Retail** platform team. The business streams point-of-sale
transactions into Kafka and wants to enrich each order in real time. The enrichment must
fan out to multiple processors so that different business rules can run in parallel,
then fan back in to a single stream that downstream services can consume without caring
about how the enrichment was done.

Velocity Retail has identified three enrichment paths that should run concurrently:
1. **Fraud check** – flag orders above a configurable amount or placed from blocked
   stores.
2. **Loyalty scoring** – compute loyalty points based on order category and purchase
   frequency.
3. **Fulfilment prep** – normalise shipping windows and mark inventory reservations.

The existing `TopologyBuilder` already demonstrates how to branch streams and merge them
again, but it still uses placeholder even/odd logic. Your goal is to turn this module
into a realistic showcase that the team can use as a starter project for new hires.

### Requirements
Implement the following features inside the fanout-fanin module:

1. **Define the payload** – Order events arrive as JSON with the shape:
   ```json
   {
     "orderId": "1f52",
     "storeId": "denver-05",
     "channel": "ONLINE",
     "category": "ELECTRONICS",
     "amount": 249.99,
     "customerId": "cust-7788",
     "ordersLast30Days": 7,
     "requestedShipDate": "2024-05-24"
   }
   ```
   Parse the payload into a domain type so you can safely access fields in later steps.

2. **Branch the stream** – Replace the even/odd predicate with three branches:
   - Fraud branch: `amount` greater than a configurable threshold _or_ `storeId` listed
     in a `blocked.stores` configuration property.
   - Loyalty branch: all other events (acts as catch-all) that should accrue points.
   - Fulfilment branch: events whose `channel` is `ONLINE` and `requestedShipDate` is
     within two days of the current processing time.

   Kafka Streams requires all predicates to be supplied at once; make sure the order of
   predicates matches the expected priority (fraud > fulfilment > loyalty fallback).

3. **Enrich each branch** – Transform each branch independently:
   - Fraud branch: attach a `fraudReviewRequired` boolean and the rule that fired.
   - Loyalty branch: calculate points using a simple multiplier table (e.g., electronics
     get 3× amount, groceries 1×, other categories 2×).
   - Fulfilment branch: attach a `packingDeadline` timestamp equal to
     `requestedShipDate` minus 12 hours.

   Use immutable value objects (e.g., record classes) for the intermediate results.

4. **Merge results** – Merge the branches back into a single stream of JSON enriched
   events. Each output record should include:
   - The original order payload.
   - The enrichment block(s) that ran.
   - A `processingTrace` array documenting which branch handled the record.

   Ensure you preserve the original key (`orderId`).

5. **Testing** – Add unit tests with `TopologyTestDriver` covering:
   - Fraud detection for high-value and blocked-store orders.
   - Loyalty scoring for at least two categories.
   - Fulfilment deadlines for overnight orders.

6. **Configuration** – Make threshold, blocked stores, and loyalty multiplier table
   configurable via system properties or application configuration files. Demonstrate
   sensible defaults so the topology runs out of the box.

### Deliverables
- Updated production code in `fanout-fanin/src/main/java/**` implementing the new
  topology and supporting types.
- Tests in `fanout-fanin/src/test/java/**` demonstrating the behaviour described above.
- Documentation updates (this README) summarising the new business context and how to
  run the challenge.

### Stretch ideas (optional)
- Add metrics or logging around branch throughput.
- Publish suspected fraud events to a dedicated side topic.
- Introduce a state store to deduplicate orders by `orderId` + `customerId`.

### How to submit
1. Fork the repository and implement the requirements above.
2. Include a `DESIGN.md` summarising the trade-offs you made.
3. Provide instructions for running the topology locally (e.g., Docker + Kafka setup).
4. Open a pull request describing your approach, test coverage, and follow-up ideas.

Focus on clear, maintainable code and a compelling explanation of your design choices.
