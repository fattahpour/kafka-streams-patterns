package com.fattahpour.kstreamspatterns.pipelinestrangler;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PipelineStranglerTopologyTest {

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<PipelineEvent> eventSerde = JsonSerdes.serde(PipelineEvent.class);

  @AfterEach
  void clearProps() {
    System.clearProperty("strangler.mode");
  }

  @Test
  void duplicatesTrafficInDualMode() {
    System.setProperty("strangler.mode", "dual");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = PipelineStranglerTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "strangler-dual");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, PipelineEvent> input =
          driver.createInputTopic(
              "pattern.pipeline-strangler.input",
              stringSerde.serializer(),
              eventSerde.serializer());
      TestOutputTopic<String, PipelineEvent> legacy =
          driver.createOutputTopic(
              "pattern.pipeline-strangler.legacy",
              stringSerde.deserializer(),
              eventSerde.deserializer());
      TestOutputTopic<String, PipelineEvent> modern =
          driver.createOutputTopic(
              "pattern.pipeline-strangler.modern",
              stringSerde.deserializer(),
              eventSerde.deserializer());

      PipelineEvent event = new PipelineEvent("evt-1", "payload", "corr-1");
      input.pipeInput("evt-1", event);

      List<TestRecord<String, PipelineEvent>> legacyRecords = legacy.readRecordsToList();
      List<TestRecord<String, PipelineEvent>> modernRecords = modern.readRecordsToList();

      assertThat(legacyRecords).hasSize(1);
      assertThat(modernRecords).hasSize(1);
      assertThat(metrics.ingress()).isEqualTo(1);
      assertThat(metrics.legacyRouted()).isEqualTo(1);
      assertThat(metrics.modernRouted()).isEqualTo(1);
    }
  }

  @Test
  void routesOnlyLegacyWhenFlagged() {
    System.setProperty("strangler.mode", "legacy");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = PipelineStranglerTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "strangler-legacy");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, PipelineEvent> input =
          driver.createInputTopic(
              "pattern.pipeline-strangler.input",
              stringSerde.serializer(),
              eventSerde.serializer());
      TestOutputTopic<String, PipelineEvent> legacy =
          driver.createOutputTopic(
              "pattern.pipeline-strangler.legacy",
              stringSerde.deserializer(),
              eventSerde.deserializer());
      TestOutputTopic<String, PipelineEvent> modern =
          driver.createOutputTopic(
              "pattern.pipeline-strangler.modern",
              stringSerde.deserializer(),
              eventSerde.deserializer());

      input.pipeInput("evt-2", new PipelineEvent("evt-2", "payload", null));

      assertThat(legacy.readValuesToList()).hasSize(1);
      assertThat(modern.isEmpty()).isTrue();
      assertThat(metrics.ingress()).isEqualTo(1);
      assertThat(metrics.legacyRouted()).isEqualTo(1);
      assertThat(metrics.modernRouted()).isZero();
    }
  }
}
