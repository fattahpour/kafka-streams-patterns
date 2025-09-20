# Event Gateway Connect Pattern

Wrap Kafka Connect style pipelines with guard-rails: schema validation, retries with back-off, and
DLQ routing.

## Intent

Incoming envelopes are validated against an expected schema version. Valid payloads go through a
processing transformer that either succeeds, requests a retry with headers that encode the next
attempt and back-off, or punts irrecoverable records to a dead-letter topic.

## Topology

```
(pattern.event-gateway-connect.in)
        |
        v
    [ validate schema ] --invalid--> (pattern.event-gateway-connect.dlq)
        |
        v
    [ process + retry policy ]
        |            |
        v            v
(pattern.event-gateway-connect.out)   (pattern.event-gateway-connect.retry)
```

## Configuration

* Topics: `pattern.event-gateway-connect.in`, `.out`, `.retry`, `.dlq`
* Headers: `correlation-id`, `causation-id`, `retry-attempt`, `retry-backoff-ms`
* Metrics: processed total, successful dispatches, retry count, DLQ count
* Tunables: `gateway.retry.max`, `gateway.retry.backoff.ms`

Defaults are provided under `src/main/resources/application.properties`.

## Running

```bash
make -C event-gateway-connect build
make -C event-gateway-connect run \
  gateway.retry.max=5 \
  gateway.retry.backoff.ms=2000
```

## Testing

Unit tests cover:

* Happy path routing to the success topic
* Schema validation failures landing on the DLQ
* Temporary failures that trigger retry headers and metrics

Run the suite with:

```bash
make -C event-gateway-connect test
```
