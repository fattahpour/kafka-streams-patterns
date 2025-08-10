# branch-route

Branch an input stream into multiple topics using predicates.

## Problem
An IoT hub receives status updates from thousands of devices and must separate critical
alerts from routine metrics.

## Solution
Branching predicates route urgent events to an incident topic for immediate action while
normal metrics flow to a stream for batch reporting.

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
