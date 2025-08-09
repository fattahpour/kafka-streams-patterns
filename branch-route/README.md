# branch-route

Branch an input stream into multiple topics using predicates.

```
input-branch ---> branch ---+--> even-branch
                            \
                             \--> odd-branch
```

## How to run

```bash
docker compose up -d
./branch-route/run.sh
```

Produces numbers to `input-branch` and observe routed values on `even-branch` and `odd-branch`.
