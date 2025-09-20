# Event Chunking Pattern

Split oversized records into sequenced fragments and reassemble them with stateful coordination.

## Intent

A splitter breaks an input message into fixed-size chunks, adds chunk sequencing headers, and
writes them to a shared topic. A merge stage buffers fragments per key until all parts arrive or a
timeout expires.

## Topology

```
(pattern.event-chunking.in)
        |
        v
  [ split into N chunks ] --(chunks)--> (pattern.event-chunking.chunks)
                                            |
                                            v
                                 [ merge + timeout guard ]
                                     |             |
                                     v             v
                          (pattern.event-chunking.out)   (pattern.event-chunking.expired)
```

## Configuration

* Topics: `pattern.event-chunking.in`, `pattern.event-chunking.chunks`, `pattern.event-chunking.out`, `pattern.event-chunking.expired`
* Headers: `correlation-id`, `chunk-seq`, `chunk-total`
* Metrics: chunk count, reassembled messages, duplicate count, timeout count
* Tunables: chunk size (`chunk.size`) and reassembly timeout (`chunk.timeout.ms`)

Default values are defined in `src/main/resources/application.properties`.

## Running

```bash
make -C event-chunking build
make -C event-chunking run \
  input.topic=pattern.event-chunking.in \
  chunk.size=256 \
  chunk.timeout.ms=120000
```

## Testing

The test suite verifies:

* End-to-end chunking and reassembly for a large payload
* Duplicate chunk suppression (only new fragments counted)
* State cleanup and timeout behaviour when fragments go missing

Execute the tests via:

```bash
make -C event-chunking test
```
