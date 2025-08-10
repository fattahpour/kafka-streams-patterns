# retry-dlq

Retry pattern using backoff topics and a dead-letter queue with attempt headers.

## Problem
A payment gateway occasionally times out when calling a bank API.

## Solution
The stream retries the charge with exponential backoff and, after all attempts fail, sends
the event to a dead-letter topic for manual review.

```
input/retry-1/retry-2 -> [RetryProcessor] -> success
                               |\
                               | \-> retry-1
                               |--\-> retry-2
                                 \-> dlq
```

## How to run

```bash
mvn -pl retry-dlq -am clean package
java -jar retry-dlq/target/retry-dlq-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=retry-dlq-app \
  -Dinput.topic=input-retry \
  -Dretry1.topic=retry-1 \
  -Dretry2.topic=retry-2 \
  -Ddlq.topic=dlq \
  -Doutput.topic=output-retry
```
