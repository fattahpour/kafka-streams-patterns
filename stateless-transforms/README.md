# Stateless Transforms Pattern

Demonstrates map/filter/flatMap on a stream.

## Scenario

Perform lightweight cleansing and expansion of events, like uppercasing messages and splitting them into individual words.

Topology:
```
input -> MAP(toUpperCase) -> FILTER(!startsWith("IGNORE")) -> FLATMAP(split) -> output
```

## How to run

```bash
mvn -pl stateless-transforms -am clean package
java -jar stateless-transforms/target/stateless-transforms-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=stateless-transforms-app \
  -Dinput.topic=input-stateless \
  -Doutput.topic=output-stateless
```
