# suppression

Emit final windowed results only using `Suppressed.untilWindowCloses`.

## Topology

```
input-suppression --> [group & count in tumbling window] --> suppress until window closes --> output-suppression
```

## Running

```bash
./run.sh
```
