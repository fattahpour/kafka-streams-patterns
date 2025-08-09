# retry-dlq

Retry pattern using backoff topics and a dead-letter queue with attempt headers.

```
input/retry-1/retry-2 -> [RetryProcessor] -> success
                               |\
                               | \-> retry-1
                               |--\-> retry-2
                                 \-> dlq
```

### Running

```
./run.sh
```
