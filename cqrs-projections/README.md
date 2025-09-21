# CQRS Projections Pattern

Apply command messages to update aggregate state and publish projection snapshots downstream.

## Intent

Commands flow into a stateful processor that enforces simple business rules (create, update,
delete). Valid transitions update a projection store and emit a snapshot event. Invalid or
out-of-order commands are directed to a DLQ.

## Topology

```
(pattern.cqrs.commands)
        |
        v
   [ validate ] ----> (pattern.cqrs.dlq)
        |
        v
 [ apply command ] --> (pattern.cqrs.events)
        |
        +-- state store: projection-store
```

## Configuration

* Topics: `.commands`, `.events`, `.dlq`
* State store: `projection-store`

Default topic names live in `src/main/resources/application.properties`.

## Running

```bash
make -C cqrs-projections build
make -C cqrs-projections run
```

## Testing

Tests assert that:

* Create/update commands produce projection snapshots and advance state
* Updates without prior state are rejected and routed to the DLQ

Run the suite with:

```bash
make -C cqrs-projections test
```
