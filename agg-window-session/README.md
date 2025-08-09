# agg-window-session

Count records per key in sessions with a one-minute inactivity gap.

```
input --> groupByKey --> sessionWindow(1m).count --> output
```

## How to run

```bash
docker compose up -d
./agg-window-session/run.sh
```

Produce records on `input` and observe counts on `session-count`.
