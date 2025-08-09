package io.example.kstreamspatterns.joinkstreamktable;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void joinsWhenTableHasKey() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> tableTopic =
          testDriver.createInputTopic(
              "table-join", Serdes.String().serializer(), Serdes.String().serializer());
      TestInputTopic<String, String> streamTopic =
          testDriver.createInputTopic(
              "stream-join", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> outputTopic =
          testDriver.createOutputTopic(
              "joined", Serdes.String().deserializer(), Serdes.String().deserializer());
      tableTopic.pipeInput("k1", "T1");
      streamTopic.pipeInput("k1", "S1");
      assertThat(outputTopic.readValuesToList()).containsExactly("S1:T1");
    }
  }

  @Test
  void dropsWhenTableMissingKey() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> streamTopic =
          testDriver.createInputTopic(
              "stream-join", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> outputTopic =
          testDriver.createOutputTopic(
              "joined", Serdes.String().deserializer(), Serdes.String().deserializer());
      streamTopic.pipeInput("k2", "S2");
      assertThat(outputTopic.isEmpty()).isTrue();
    }
  }
}
