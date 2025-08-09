# join-ktable-ktable

Join two tables and emit the joined values.

```
table-a ---\
           +--> join --> joined
 table-b --/
```

## How to run

```bash
docker compose up -d
./join-ktable-ktable/run.sh
```

Produce records on `table-a` and `table-b` and observe joined values on `joined`.
