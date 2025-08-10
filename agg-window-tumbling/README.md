# agg-window-tumbling

Count records per key in one-minute tumbling windows.

## Problem
A telecom provider must monitor network pings per tower to detect outages.

## Solution
One-minute tumbling windows create discrete buckets so analysts can spot sustained drops
in connectivity.

```
input --> groupByKey --> window(1m).count --> output
```

## How to run

```bash
mvn -pl agg-window-tumbling -am clean package
java -jar agg-window-tumbling/target/agg-window-tumbling-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=agg-window-tumbling \
  -Dinput.topic=tumbling-input \
  -Doutput.topic=tumbling-output
```

Produce records on `tumbling-input` and observe counts on `tumbling-output`.
