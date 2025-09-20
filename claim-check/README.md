# Claim Check Pattern

Store large payloads outside Kafka and only pass lightweight references between services.

## Intent

The producer writes blob data to a file-backed `ClaimCheckStore` and publishes a reference
message. Downstream consumers resolve the claim by loading the payload and falling back when
blobs are missing.

## Topology

```
(pattern.claim-check.in)
        |
        v
 [ store payload ] --(claim uri)--> (pattern.claim-check.refs)
        |                                  |
        +-------------------------through---+
                                           v
                                   [ resolve claim ]
                                           |
                                           v
                                (pattern.claim-check.out)
```

## Configuration

* Topics: `pattern.claim-check.in`, `pattern.claim-check.refs`, `pattern.claim-check.out`, `pattern.claim-check.dlq`
* Headers: `correlation-id`, `causation-id`, and `claim-check-uri`
* Metrics: processed count, references produced, resolved count, fallbacks, DLQ count
* Store: filesystem path defined by `claim.check.store` (defaults to `/tmp/blobstore`)

`src/main/resources/application.properties` contains runnable defaults.

## Running

```bash
make -C claim-check build
make -C claim-check run \
  input.topic=pattern.claim-check.in \
  references.topic=pattern.claim-check.refs \
  output.topic=pattern.claim-check.out
```

## Testing

The module includes four JUnit/TopologyTestDriver suites covering:

* Reference emission without leaking the blob contents
* Successful claim resolution round-trips
* Fallback behaviour when payloads are missing
* DLQ routing for malformed input

Run the tests with:

```bash
make -C claim-check test
```
