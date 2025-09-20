# Logical AND / Multi-Signal Pattern

Correlate multiple signal types per key and emit a result only when the required set arrives within
a time window.

## Intent

Signals A, B, and C (configurable via code) flow through a correlation transformer backed by a
state store. The processor waits for all three unique signal types within a window, emits a single
aggregated event, and drops partial sets that expire.

## Topology

```
(pattern.logical-and-multisignal.in)
        |
        v
    [ correlate signals ] --complete--> (pattern.logical-and-multisignal.out)
        |
        +-- expired --> (pattern.logical-and-multisignal.expired)
```

## Configuration

* Topics: `.in`, `.out`, `.expired`
* Headers: `correlation-id`
* Metrics: processed signal count, completed correlations, expired correlations
* Window: `correlation.window.ms`

Default settings live in `src/main/resources/application.properties`.

## Running

```bash
make -C logical-and-multisignal build
make -C logical-and-multisignal run \
  correlation.window.ms=30000
```

## Testing

The test suite asserts:

* Successful emission when all three signals arrive in time
* Expiration of partial sets after the configured timeout
* Duplicate signals for the same key do not trigger duplicate outputs

Run the tests with:

```bash
make -C logical-and-multisignal test
```
