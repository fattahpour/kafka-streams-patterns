package com.fattahpour.kstreamspatterns.eventchunking;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class EventChunkingTopologyTest {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<ChunkableEvent> EVENT_SERDE = JsonSerdes.serde(ChunkableEvent.class);
  private static final Serde<EventChunk> CHUNK_SERDE = JsonSerdes.serde(EventChunk.class);
  private static final Serde<ReassembledEvent> REASSEMBLED_SERDE = JsonSerdes.serde(ReassembledEvent.class);
  private static final Serde<ChunkTimeout> TIMEOUT_SERDE = JsonSerdes.serde(ChunkTimeout.class);

  @Test
  void reassemblesChunksFromLargePayload() {
    System.setProperty("chunk.size", "5");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = EventChunkingTopology.build(metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, ChunkableEvent> input =
          driver.createInputTopic("pattern.event-chunking.in", STRING_SERDE.serializer(), EVENT_SERDE.serializer());
      TestOutputTopic<String, ReassembledEvent> output =
          driver.createOutputTopic(
              "pattern.event-chunking.out", STRING_SERDE.deserializer(), REASSEMBLED_SERDE.deserializer());

      ChunkableEvent event = new ChunkableEvent("order-1", "abcdefghij", "corr-1");
      input.pipeInput("order-1", event);

      ReassembledEvent result = output.readValue();
      assertThat(result.payload()).isEqualTo("abcdefghij");
      assertThat(metrics.reassembled()).isEqualTo(1);
    } finally {
      System.clearProperty("chunk.size");
    }
  }

  @Test
  void duplicateChunksAreIgnored() {
    System.setProperty("chunk.timeout.ms", "10000");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = EventChunkingTopology.build(metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, EventChunk> chunks =
          driver.createInputTopic(
              "pattern.event-chunking.chunks", STRING_SERDE.serializer(), CHUNK_SERDE.serializer());
      TestOutputTopic<String, ReassembledEvent> output =
          driver.createOutputTopic(
              "pattern.event-chunking.out", STRING_SERDE.deserializer(), REASSEMBLED_SERDE.deserializer());

      EventChunk first = new EventChunk("dup-1", 0, 3, "AAA", "corr-dup");
      EventChunk second = new EventChunk("dup-1", 1, 3, "BBB", "corr-dup");
      EventChunk third = new EventChunk("dup-1", 2, 3, "CCC", "corr-dup");

      chunks.pipeInput("dup-1", first);
      chunks.pipeInput("dup-1", first); // duplicate
      chunks.pipeInput("dup-1", second);
      chunks.pipeInput("dup-1", third);

      ReassembledEvent result = output.readValue();
      assertThat(result.payload()).isEqualTo("AAABBBCCC");
      assertThat(output.isEmpty()).isTrue();
      assertThat(metrics.duplicates()).isEqualTo(1);
    } finally {
      System.clearProperty("chunk.timeout.ms");
    }
  }

  @Test
  void missingChunkTriggersTimeout() {
    System.setProperty("chunk.timeout.ms", "500");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = EventChunkingTopology.build(metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, EventChunk> chunks =
          driver.createInputTopic(
              "pattern.event-chunking.chunks", STRING_SERDE.serializer(), CHUNK_SERDE.serializer());
      TestOutputTopic<String, ChunkTimeout> expired =
          driver.createOutputTopic(
              "pattern.event-chunking.expired", STRING_SERDE.deserializer(), TIMEOUT_SERDE.deserializer());
      TestOutputTopic<String, ReassembledEvent> output =
          driver.createOutputTopic(
              "pattern.event-chunking.out", STRING_SERDE.deserializer(), REASSEMBLED_SERDE.deserializer());

      EventChunk first = new EventChunk("timeout-1", 0, 2, "PAYLOAD", "corr-timeout");
      chunks.pipeInput("timeout-1", first);

      driver.advanceWallClockTime(Duration.ofMillis(600));

      assertThat(expired.readValue().id()).isEqualTo("timeout-1");
      assertThat(output.isEmpty()).isTrue();
      assertThat(metrics.timeouts()).isEqualTo(1);
    } finally {
      System.clearProperty("chunk.timeout.ms");
    }
  }

  private Properties props() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-chunking-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    return properties;
  }
}
