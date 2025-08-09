# agg-window-hopping

Count records per key in one-minute windows advancing every 30 seconds.

```
input --> groupByKey --> window(1m,30s).count --> output
```

## How to run

```bash
docker compose up -d
./agg-window-hopping/run.sh
```

Produce records on `input` and observe counts on `hopping-count`.
