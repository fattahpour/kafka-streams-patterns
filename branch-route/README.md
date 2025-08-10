# branch-route

Branch an input stream into multiple topics using predicates.

## Scenario

Route transactions to specialized topics based on their attributes, for example separating credit and debit operations.

```
input-branch ---> branch ---+--> even-branch
                            \
                             \--> odd-branch
```

## How to run

```bash
mvn -pl branch-route -am clean package
java -jar branch-route/target/branch-route-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=branch-route \
  -Dinput.topic=input-branch \
  -Deven.topic=even-branch \
  -Dodd.topic=odd-branch
```

Produces numbers to `input-branch` and observe routed values on `even-branch` and `odd-branch`.
