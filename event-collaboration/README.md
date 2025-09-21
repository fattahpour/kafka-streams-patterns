# Event Collaboration Pattern

Correlate heterogeneous event streams (alpha and beta) while tolerating limited out-of-order
arrival.

## Intent

Two upstream services emit complementary facts. A collaboration transformer buffers partial data in
an in-memory store and emits a unified result when both sides arrive within a configurable
lateness window. Events that exceed the lateness budget are emitted to a dedicated late stream for
manual handling.

## Topology

```
(pattern.event-collaboration.alpha)    (pattern.event-collaboration.beta)
                 |                               |
                 +----------- merge -------------+
                                 |
                                 v
                    [ collaborate + tolerate late ]
                         |                    |
                         v                    v
      (pattern.event-collaboration.joined)  (pattern.event-collaboration.late)
```

## Configuration

* Topics: `.alpha`, `.beta`, `.joined`, `.late`
* Property: `collaboration.lateness.ms` (default `5000`) – maximum tolerated time difference

The defaults reside in `src/main/resources/application.properties`.

## Running

```bash
make -C event-collaboration build
make -C event-collaboration run \
  collaboration.lateness.ms=10000
```

## Testing

Tests ensure:

* Alpha and beta events within the lateness window produce a single joined event
* Events arriving outside the lateness window are diverted to the late stream

Execute the tests with:

```bash
make -C event-collaboration test
```
