# agg-window-tumbling

Count records per key in one-minute tumbling windows.

```
input --> groupByKey --> window(1m).count --> output
```

## How to run

```bash
docker compose up -d
./agg-window-tumbling/run.sh
```

Produce records on `input` and observe counts on `tumbling-count`.
