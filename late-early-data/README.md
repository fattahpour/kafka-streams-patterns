# late-early-data

Emit early and final windowed counts while dropping records arriving after the grace period.

## Problem
Sensor networks emit readings at irregular times, requiring quick insight without losing
late updates.

## Solution
Early results are emitted immediately, and late data updates counts until the grace period
expires to balance speed and accuracy.

```
input --> groupByKey --> window(5s, grace 5s).count.suppress --> output
```

## How to run

```bash
mvn -pl late-early-data -am clean package
java -jar late-early-data/target/late-early-data-1.0.0-SNAPSHOT.jar \
  -Dinput.topic=late-early-input \
  -Dearly.topic=late-early-early \
  -Doutput.topic=late-early-output
```

Produce records on `late-early-input` and observe early and final counts on `late-early-output`.

## Generate example data

```bash
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator late-early-data
```
