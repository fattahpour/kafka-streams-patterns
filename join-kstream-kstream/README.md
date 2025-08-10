# join-kstream-kstream

Join two streams within a five-minute window.

## Problem
An ad platform needs to correlate impression events with clicks that arrive within five
minutes.

## Solution
Joining the two streams attributes engagement and measures campaign effectiveness.

```
left-join ----\
              +--> join --> joined
right-join ---/
```

## How to run

```bash
mvn -pl join-kstream-kstream -am clean package
java -jar join-kstream-kstream/target/join-kstream-kstream-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=join-kstream-kstream \
  -Dleft.topic=left-join \
  -Dright.topic=right-join \
  -Doutput.topic=joined
```

Produce matching records on `left-join` and `right-join` and observe joined values on `joined`.

## Generate example data

```bash
mvn -pl common -am package
java -cp common/target/common-1.0.0-SNAPSHOT.jar \
  com.fattahpour.kstreamspatterns.common.FakeDataGenerator join-kstream-kstream
```
