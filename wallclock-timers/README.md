# wallclock-timers

**Pattern:** Offload cron-style scheduling into Kafka Streams so timers survive restarts and emit
structured events for downstream processing. `ScheduleCommand` records go in, persistent state keeps
track of due times, and a wall-clock punctuator emits `TimerFiredEvent` records once timestamps are
reached. Invalid commands are routed to a DLQ for observability.

## Problem
Teams need reliable reminders to run long after a request is created. Cron jobs are brittle and
distributed services must survive short restarts without losing scheduled work.

## Solution
Persist each schedule command in a state store and rely on a wall-clock punctuator to fire due
entries. If a command is malformed it is immediately routed to a DLQ.

```
commands --> validate --> store --> punctuate --> fired
                 |                          \
                 +----> dlq                   +--> metrics
```

### Topics

- **`pattern.wallclock-timers.commands`** – ingress topic that accepts `ScheduleCommand` records.
  Each command provides a stable `id`, the target wall-clock timestamp (`dueAt` in epoch millis),
  optional payload, and correlation id that is echoed when the timer fires.
- **`pattern.wallclock-timers.fired`** – contains `TimerFiredEvent` records for timers whose due
  time has passed. The event includes the original payload, the `dueAt` timestamp, and the actual
  `firedAt` time chosen by Streams. Headers also carry the original correlation id and timer id for
  downstream tracing.
- **`pattern.wallclock-timers.dlq`** – collects `TimerError` records whenever a command is
  rejected. Reasons include missing identifiers, negative `dueAt` values, or other validation
  issues. Monitoring this topic helps operators spot faulty clients before they flood the system.

## How to run

```bash
make -C wallclock-timers build
make -C wallclock-timers run timer.check.ms=500 timer.grace.ms=100
```

## Testing

```bash
make -C wallclock-timers test
```

Tests assert that timers fire when the wall clock advances and that invalid commands go to the DLQ.
