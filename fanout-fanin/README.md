# Fanout Fanin Pattern

Demonstrates splitting a stream into branches and merging the results.

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
