# agg-window-session

Count records per key in sessions with a one-minute inactivity gap.

## Problem
Marketing at an e-commerce portal needs to know how long shoppers stay engaged.

## Solution
Session windows group clicks until a minute of inactivity ends the visit, yielding
per-session counts that power personalized offers.

```
input --> groupByKey --> sessionWindow(1m).count --> output
```

## How to run

```bash
mvn -pl agg-window-session -am clean package
java -jar agg-window-session/target/agg-window-session-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=agg-window-session \
  -Dinput.topic=session-input \
  -Doutput.topic=session-output
```

Produce records on `session-input` and observe counts on `session-output`.

## Generate example data

```bash
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator agg-window-session
```
