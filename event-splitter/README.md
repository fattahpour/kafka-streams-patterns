# Event Splitter Pattern

Split a composite event into multiple child events while preserving lineage via headers.

## Intent

A composite payload contains several logical sub-records. The splitter validates the envelope,
explodes it into independent child events, and annotates each output with lineage headers so
consumers can trace back to the original aggregate event. Malformed envelopes are routed to a DLQ.

## Topology

```
(pattern.event-splitter.in)
        |
        v
   [ validate ] ----> (pattern.event-splitter.dlq)
        |
        v
   [ split + annotate ] --> (pattern.event-splitter.children)
```

## Configuration

* Topics: `.in`, `.children`, `.dlq`
* Headers: `lineage-parent-id`, `lineage-child-index`

All defaults live in `src/main/resources/application.properties` and can be overridden via system
properties.

## Running

```bash
make -C event-splitter build
make -C event-splitter run
```

## Testing

Tests assert:

* Composite events fan out into ordered child events with the expected lineage headers
* Empty fragment lists are rejected and forwarded to the DLQ with a reason code

Execute the suite with:

```bash
make -C event-splitter test
```
