# agg-window-hopping

Count records per key in one-minute windows advancing every 30 seconds.

## Problem
An ops team for a busy news site must spot sudden traffic surges so they can scale
servers before readers experience slowdowns.

## Solution
Hopping windows count visits every minute with a 30-second slide, surfacing spikes
early enough to trigger proactive scaling.

```
input --> groupByKey --> window(1m,30s).count --> output
```

## How to run

```bash
mvn -pl agg-window-hopping -am clean package
java -jar agg-window-hopping/target/agg-window-hopping-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=agg-window-hopping \
  -Dinput.topic=input \
  -Doutput.topic=hopping-count
```

Produce records on `input` and observe counts on `hopping-count`.
