# Saga Orchestration Pattern

Coordinate a simple order saga, emitting compensating actions when downstream steps fail.

## Intent

Orders trigger a multi-step saga: reserve inventory, charge payment, and complete the order.
Failures during payment trigger compensating actions that release inventory and publish a failure
notification. The orchestration logic lives in a single transformer for clarity.

## Topology

```
(pattern.saga.orders)
        |
        v
   [ orchestrate saga ] --> (pattern.saga.events)
        |
        +------------------> (pattern.saga.compensations)
        |
        +------------------> (pattern.saga.dlq)
```

## Configuration

* Topics: `.orders`, `.events`, `.compensations`, `.dlq`

Defaults exist in `src/main/resources/application.properties` and can be overridden via system
properties.

## Running

```bash
make -C saga-orchestration build
make -C saga-orchestration run
```

## Testing

The tests verify:

* Happy path orders emit the expected saga events (inventory reserved, payment charged, order
  completed)
* Orders flagged to fail trigger compensation and an order failure event

Run the tests with:

```bash
make -C saga-orchestration test
```
