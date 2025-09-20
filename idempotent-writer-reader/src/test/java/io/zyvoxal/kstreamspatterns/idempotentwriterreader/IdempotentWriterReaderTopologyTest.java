package io.zyvoxal.kstreamspatterns.idempotentwriterreader;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class IdempotentWriterReaderTopologyTest {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<InboundEvent> INBOUND_SERDE = JsonSerdes.serde(InboundEvent.class);
  private static final Serde<DeduplicatedEvent> DEDUP_SERDE = JsonSerdes.serde(DeduplicatedEvent.class);
  private static final Serde<ProcessedEvent> PROCESSED_SERDE = JsonSerdes.serde(ProcessedEvent.class);
  private static final Serde<DlqRecord> DLQ_SERDE = JsonSerdes.serde(DlqRecord.class);

  @Test
  void writerFiltersDuplicateEvents() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = IdempotentWriterReaderTopology.build(metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, InboundEvent> input =
          driver.createInputTopic(
              "pattern.idempotent-writer-reader.in", STRING_SERDE.serializer(), INBOUND_SERDE.serializer());
      TestOutputTopic<String, DeduplicatedEvent> writerOutput =
          driver.createOutputTopic(
              "pattern.idempotent-writer-reader.writer", STRING_SERDE.deserializer(), DEDUP_SERDE.deserializer());

      InboundEvent event = new InboundEvent("evt-1", "payload", "corr-1");
      input.pipeInput("evt-1", event);
      input.pipeInput("evt-1", event);

      assertThat(writerOutput.readValuesToList()).hasSize(1);
      assertThat(metrics.writerEmitted()).isEqualTo(1);
    }
  }

  @Test
  void readerSuppressesDuplicatesAcrossRestarts() throws IOException {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = IdempotentWriterReaderTopology.build(metrics);
    Path stateDir = Files.createTempDirectory("writer-reader-test");
    Properties props = props();
    props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, InboundEvent> input =
          driver.createInputTopic(
              "pattern.idempotent-writer-reader.in", STRING_SERDE.serializer(), INBOUND_SERDE.serializer());
      TestOutputTopic<String, ProcessedEvent> output =
          driver.createOutputTopic(
              "pattern.idempotent-writer-reader.out", STRING_SERDE.deserializer(), PROCESSED_SERDE.deserializer());
      input.pipeInput("evt-2", new InboundEvent("evt-2", "payload", "corr-2"));
      assertThat(output.readValuesToList()).hasSize(1);
    }

    PatternMetrics secondMetrics = new PatternMetrics();
    Topology secondTopology = IdempotentWriterReaderTopology.build(secondMetrics);
    try (TopologyTestDriver driver = new TopologyTestDriver(secondTopology, props)) {
      TestInputTopic<String, InboundEvent> input =
          driver.createInputTopic(
              "pattern.idempotent-writer-reader.in", STRING_SERDE.serializer(), INBOUND_SERDE.serializer());
      TestOutputTopic<String, ProcessedEvent> output =
          driver.createOutputTopic(
              "pattern.idempotent-writer-reader.out", STRING_SERDE.deserializer(), PROCESSED_SERDE.deserializer());
      input.pipeInput("evt-2", new InboundEvent("evt-2", "payload", "corr-2"));
      assertThat(output.isEmpty()).isTrue();
      assertThat(secondMetrics.readerEmitted()).isEqualTo(0);
    }
  }

  @Test
  void nullEventIdRoutedToDlq() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = IdempotentWriterReaderTopology.build(metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, InboundEvent> input =
          driver.createInputTopic(
              "pattern.idempotent-writer-reader.in", STRING_SERDE.serializer(), INBOUND_SERDE.serializer());
      TestOutputTopic<String, DlqRecord> dlq =
          driver.createOutputTopic(
              "pattern.idempotent-writer-reader.dlq", STRING_SERDE.deserializer(), DLQ_SERDE.deserializer());

      input.pipeInput("evt-3", new InboundEvent(null, "payload", "corr-3"));

      DlqRecord record = dlq.readValue();
      assertThat(record.reason()).isEqualTo("missing-event-id");
      assertThat(metrics.dlq()).isEqualTo(1);
    }
  }

  private Properties props() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "idempotent-writer-reader-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    return properties;
  }
}
