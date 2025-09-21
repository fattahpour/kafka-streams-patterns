# Content Filter Pattern

Early-drop noisy events before they reach heavy downstream processing. The filter evaluates payload
size and banned keywords, emitting clean events to one topic and rejected events to another.

## Intent

* Reject events whose payload contains banned terms (configured via `filter.banned.words`)
* Reject events whose payload length exceeds `filter.max.length`
* Publish accepted events to the clean topic and rejected ones to the DLQ
* Maintain metrics for processed, accepted, and dropped events

## Topology

```
(pattern.content-filter.in)
        |
        v
   [ inspect + score ]
        |             |
        v             v
(pattern.content-filter.clean)
(pattern.content-filter.dropped)
```

## Running

```bash
make -C content-filter build
make -C content-filter run \
  filter.banned.words=spam,fraud \
  filter.max.length=512
```

## Testing

The tests cover acceptance of a clean event and rejection of a banned/heavy payload.

```bash
make -C content-filter test
```
