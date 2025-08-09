# join-kstream-kstream

Join two streams within a five-minute window.

```
left-join ----\
              +--> join --> joined
right-join ---/
```

## How to run

```bash
docker compose up -d
./join-kstream-kstream/run.sh
```

Produces matching records on `left-join` and `right-join` and observe joined values on `joined`.
