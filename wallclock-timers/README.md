# Wall-Clock Timers Pattern

Schedule and fire timers based on wall-clock time using Kafka Streams punctuators.

## Intent

Commands schedule future timer executions. A stateful transformer stores pending timers and a
wall-clock punctuator scans for due entries, emitting a "timer fired" event per execution. Invalid
commands are diverted to a DLQ.

## Topology

```
(pattern.wallclock-timers.commands)
        |
        v
  [ validate ] ----> (pattern.wallclock-timers.dlq)
        |
        v
[ schedule + punctuate ] --due--> (pattern.wallclock-timers.fired)
```

## Configuration

* Topics: `.commands`, `.fired`, `.dlq`
* Properties:
  * `timer.check.ms` – wall-clock sweep interval (default `1000`)
  * `timer.grace.ms` – tolerance when considering overdue timers (default `0`)

Defaults live in `src/main/resources/application.properties` and can be overridden via JVM system
properties.

## Running

```bash
make -C wallclock-timers build
make -C wallclock-timers run \
  timer.check.ms=500 \
  timer.grace.ms=100
```

## Testing

The automated tests cover:

* Scheduling and firing a timer when the wall clock advances beyond the due time
* Rejecting invalid commands (missing identifier or negative due time) and routing them to the DLQ

Run the suite with:

```bash
make -C wallclock-timers test
```
