# late-early-data

Emit early and final windowed counts while dropping records arriving after the grace period.

```
input --> groupByKey --> window(5s, grace 5s).count.suppress --> output
```

## How to run

```bash
docker compose up -d
./late-early-data/run.sh
```

Produce records on `late-early-input` and observe early and final counts on `late-early-output`.
