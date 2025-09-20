package io.zyvoxal.kstreamspatterns.eventchunking;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public final class EventChunkingTopology {
  private static final String DEFAULT_INPUT = "pattern.event-chunking.in";
  private static final String DEFAULT_CHUNKS = "pattern.event-chunking.chunks";
  private static final String DEFAULT_OUTPUT = "pattern.event-chunking.out";
  private static final String DEFAULT_EXPIRED = "pattern.event-chunking.expired";

  private EventChunkingTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<ChunkableEvent> eventSerde = JsonSerdes.serde(ChunkableEvent.class);
    Serde<EventChunk> chunkSerde = JsonSerdes.serde(EventChunk.class);
    Serde<ReassembledEvent> reassembledSerde = JsonSerdes.serde(ReassembledEvent.class);
    Serde<ChunkTimeout> timeoutSerde = JsonSerdes.serde(ChunkTimeout.class);
    Serde<ChunkAccumulatorState> stateSerde = JsonSerdes.serde(ChunkAccumulatorState.class);

    String input = System.getProperty("input.topic", DEFAULT_INPUT);
    String chunkTopic = System.getProperty("chunk.topic", DEFAULT_CHUNKS);
    String output = System.getProperty("output.topic", DEFAULT_OUTPUT);
    String expired = System.getProperty("expired.topic", DEFAULT_EXPIRED);

    int chunkSize = Integer.parseInt(System.getProperty("chunk.size", "128"));
    Duration timeout = Duration.ofMillis(Long.parseLong(System.getProperty("chunk.timeout.ms", "60000")));

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(ChunkMergeTransformer.STORE_NAME),
            Serdes.String(),
            stateSerde));

    KStream<String, ChunkableEvent> source =
        builder.stream(input, Consumed.with(Serdes.String(), eventSerde));

    KStream<String, EventChunk> chunkStream =
        source.flatMapValues(value -> PayloadChunker.split(value, chunkSize));

    chunkStream =
        chunkStream.transformValues(
            new ChunkHeaderTransformer(metrics), Named.as("chunk-header"));

    chunkStream.to(chunkTopic, Produced.with(Serdes.String(), chunkSerde));

    KStream<String, EventChunk> chunkThrough =
        builder.stream(chunkTopic, Consumed.with(Serdes.String(), chunkSerde));

    KStream<String, ChunkMergeResult> merged =
        chunkThrough.transform(
            new ChunkMergeTransformer(timeout, metrics),
            Named.as("chunk-merge"),
            ChunkMergeTransformer.STORE_NAME);

    KStream<String, ReassembledEvent> reassembled =
        merged
            .filter((key, value) -> value != null && value.reassembled() != null)
            .mapValues(ChunkMergeResult::reassembled);

    KStream<String, ChunkTimeout> timeouts =
        merged
            .filter((key, value) -> value != null && value.timeout() != null)
            .mapValues(ChunkMergeResult::timeout);

    reassembled.to(output, Produced.with(Serdes.String(), reassembledSerde));
    timeouts.to(expired, Produced.with(Serdes.String(), timeoutSerde));

    return builder.build();
  }
}
