# agg-window-session

Count records per key in sessions with a one-minute inactivity gap.

## Scenario

Capture user activity per session, such as grouping clicks until a minute of inactivity signals the session end.

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
