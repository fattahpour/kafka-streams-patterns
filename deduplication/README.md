# deduplication

Deduplicate records by key within a ten-minute window using a state store to remember previously seen keys.

## Problem
In a payment service, retries can resend the same transaction and risk double-charging
customers.

## Solution
A ten-minute cache of seen IDs drops duplicates even if upstream components replay events.

## Topology

```
input-dedup --> [deduplicate by key] --> output-dedup
```

## Running

```bash
mvn -pl deduplication -am clean package
java -jar deduplication/target/deduplication-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=deduplication \
  -Dinput.topic=input-dedup \
  -Doutput.topic=output-dedup
```
