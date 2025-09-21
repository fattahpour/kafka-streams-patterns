package com.fattahpour.kstreamspatterns.contentfilter;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ContentFilterTopologyTest {

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<ContentEvent> eventSerde = JsonSerdes.serde(ContentEvent.class);

  @AfterEach
  void clearProperties() {
    System.clearProperty("filter.banned.words");
    System.clearProperty("filter.max.length");
  }

  @Test
  void acceptsCleanEvent() {
    System.setProperty("filter.banned.words", "spam");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = ContentFilterTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "content-filter-accept");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, ContentEvent> input =
          driver.createInputTopic(
              "pattern.content-filter.in",
              stringSerde.serializer(),
              eventSerde.serializer());
      TestOutputTopic<String, ContentEvent> clean =
          driver.createOutputTopic(
              "pattern.content-filter.clean",
              stringSerde.deserializer(),
              eventSerde.deserializer());
      TestOutputTopic<String, ContentEvent> dropped =
          driver.createOutputTopic(
              "pattern.content-filter.dropped",
              stringSerde.deserializer(),
              eventSerde.deserializer());

      input.pipeInput("id-1", new ContentEvent("id-1", "hello world", "corr-1"));

      List<ContentEvent> cleanEvents = clean.readValuesToList();
      assertThat(cleanEvents).hasSize(1);
      assertThat(cleanEvents.get(0).payload()).isEqualTo("hello world");
      assertThat(dropped.isEmpty()).isTrue();
      assertThat(metrics.processed()).isEqualTo(1);
      assertThat(metrics.accepted()).isEqualTo(1);
      assertThat(metrics.dropped()).isZero();
    }
  }

  @Test
  void rejectsBannedOrHeavyPayload() {
    System.setProperty("filter.banned.words", "blocked");
    System.setProperty("filter.max.length", "10");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = ContentFilterTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "content-filter-reject");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, ContentEvent> input =
          driver.createInputTopic(
              "pattern.content-filter.in",
              stringSerde.serializer(),
              eventSerde.serializer());
      TestOutputTopic<String, ContentEvent> clean =
          driver.createOutputTopic(
              "pattern.content-filter.clean",
              stringSerde.deserializer(),
              eventSerde.deserializer());
      TestOutputTopic<String, ContentEvent> dropped =
          driver.createOutputTopic(
              "pattern.content-filter.dropped",
              stringSerde.deserializer(),
              eventSerde.deserializer());

      input.pipeInput("id-2", new ContentEvent("id-2", "blocked content", null));
      input.pipeInput("id-3", new ContentEvent("id-3", "excessively-long", null));

      assertThat(clean.isEmpty()).isTrue();
      assertThat(dropped.readValuesToList()).hasSize(2);
      assertThat(metrics.processed()).isEqualTo(2);
      assertThat(metrics.accepted()).isZero();
      assertThat(metrics.dropped()).isEqualTo(2);
    }
  }
}
