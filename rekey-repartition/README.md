# Rekey Repartition Pattern

Demonstrates selecting a new record key and forcing a repartition.

## Scenario

Change partitioning to align with upcoming operations, for example rekeying by user ID before aggregating by user.

Topology:
```
input-rekey -> SELECTKEY(extract user) -> REPARTITION -> output-rekey
```

## How to run

```bash
mvn -pl rekey-repartition -am clean package
java -jar rekey-repartition/target/rekey-repartition-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=rekey-repartition-app \
  -Dinput.topic=input-rekey \
  -Doutput.topic=output-rekey
```
