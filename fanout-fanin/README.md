# Fanout Fanin Pattern

Demonstrates splitting a stream into branches and merging the results.

## Problem
An image-processing service must run multiple analyses on each photo and combine the
results.

## Solution
It branches uploads into separate streams for face detection and object labeling, then
merges the outputs into a single metadata record.

Topology:
```
fanout-input -> branch(even / odd)
               \              \
            map(v*2)        map(v*3)
                 \            /
                  \          /
                   merge -> fanout-output
```

## How to run

```bash
mvn -pl fanout-fanin -am clean package
java -jar fanout-fanin/target/fanout-fanin-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=fanout-fanin-app \
  -Dinput.topic=fanout-input \
  -Doutput.topic=fanout-output
```

## Generate example data

```bash
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator fanout-fanin
```
