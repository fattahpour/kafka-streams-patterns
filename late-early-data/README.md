# late-early-data

Emit early and final windowed counts while dropping records arriving after the grace period.

## Scenario

Provide quick preliminary results for analytics dashboards while still producing a final corrected count once the window closes.

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
