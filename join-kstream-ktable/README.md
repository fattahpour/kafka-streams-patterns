# join-kstream-ktable

Join a stream with a table using an inner join.

```
stream-join ---\
             +--> join --> joined
table-join --/
```

## How to run

```bash
docker compose up -d
./join-kstream-ktable/run.sh
```

Produce records on `table-join` and `stream-join` and observe joined values on `joined`.
