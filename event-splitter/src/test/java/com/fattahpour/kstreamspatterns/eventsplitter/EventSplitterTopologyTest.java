package com.fattahpour.kstreamspatterns.eventsplitter;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

class EventSplitterTopologyTest {

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<CompositeEvent> compositeSerde = JsonSerdes.serde(CompositeEvent.class);
  private final Serde<ChildEvent> childSerde = JsonSerdes.serde(ChildEvent.class);
  private final Serde<SplitterError> errorSerde = JsonSerdes.serde(SplitterError.class);

  @Test
  void splitsCompositeEventIntoChildEvents() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = EventSplitterTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-splitter-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, CompositeEvent> input =
          driver.createInputTopic(
              "pattern.event-splitter.in",
              stringSerde.serializer(),
              compositeSerde.serializer());
      TestOutputTopic<String, ChildEvent> children =
          driver.createOutputTopic(
              "pattern.event-splitter.children",
              stringSerde.deserializer(),
              childSerde.deserializer());

      CompositeEvent composite =
          new CompositeEvent("parent-1", "corr-1", List.of("a", "b", "c"));
      input.pipeInput("parent-1", composite);

      List<TestRecord<String, ChildEvent>> records = children.readRecordsToList();
      assertThat(records).hasSize(3);
      for (int i = 0; i < records.size(); i++) {
        TestRecord<String, ChildEvent> record = records.get(i);
        ChildEvent child = record.value();
        assertThat(child.parentId()).isEqualTo("parent-1");
        assertThat(child.index()).isEqualTo(i);
        assertThat(child.payload()).isEqualTo(composite.fragments().get(i));
        assertThat(child.correlationId()).isEqualTo("corr-1");
        Headers headers = record.headers();
        assertThat(new String(headers.lastHeader("lineage-parent-id").value()))
            .isEqualTo("parent-1");
        assertThat(new String(headers.lastHeader("lineage-child-index").value()))
            .isEqualTo(Integer.toString(i));
      }
      assertThat(metrics.split()).isEqualTo(1);
      assertThat(metrics.fragmentsEmitted()).isEqualTo(3);
    }
  }

  @Test
  void routesInvalidEnvelopeToDlq() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = EventSplitterTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-splitter-test-invalid");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, CompositeEvent> input =
          driver.createInputTopic(
              "pattern.event-splitter.in",
              stringSerde.serializer(),
              compositeSerde.serializer());
      TestOutputTopic<String, SplitterError> dlq =
          driver.createOutputTopic(
              "pattern.event-splitter.dlq",
              stringSerde.deserializer(),
              errorSerde.deserializer());

      input.pipeInput("parent-2", new CompositeEvent("parent-2", "corr-2", List.of()));

      SplitterError error = dlq.readValue();
      assertThat(error.reason()).isEqualTo("invalid-envelope");
      assertThat(metrics.invalid()).isEqualTo(1);
    }
  }
}
