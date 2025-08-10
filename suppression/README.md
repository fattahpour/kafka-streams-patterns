# suppression

Emit final windowed results only using `Suppressed.untilWindowCloses`.

## Problem
An auction service tracks bids throughout the day but only wants to publish the final
winning price.

## Solution
Suppression buffers intermediate counts and emits a single result when the bidding window
closes.

## Topology

```
input-suppression --> [group & count in tumbling window] --> suppress until window closes --> output-suppression
```

## How to run

```bash
mvn -pl suppression -am clean package
java -jar suppression/target/suppression-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=suppression \
  -Dinput.topic=input-suppression \
  -Doutput.topic=output-suppression
```
