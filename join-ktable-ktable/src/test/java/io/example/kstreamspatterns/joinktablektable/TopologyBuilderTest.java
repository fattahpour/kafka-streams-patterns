package io.example.kstreamspatterns.joinktablektable;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void joinsWhenBothTablesHaveKey() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> aTopic =
          testDriver.createInputTopic(
              "table-a", Serdes.String().serializer(), Serdes.String().serializer());
      TestInputTopic<String, String> bTopic =
          testDriver.createInputTopic(
              "table-b", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> outputTopic =
          testDriver.createOutputTopic(
              "joined", Serdes.String().deserializer(), Serdes.String().deserializer());
      aTopic.pipeInput("k1", "A1");
      bTopic.pipeInput("k1", "B1");
      assertThat(outputTopic.readValuesToList()).containsExactly("A1:B1");
    }
  }

  @Test
  void noOutputWhenMissingKey() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> aTopic =
          testDriver.createInputTopic(
              "table-a", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> outputTopic =
          testDriver.createOutputTopic(
              "joined", Serdes.String().deserializer(), Serdes.String().deserializer());
      aTopic.pipeInput("k2", "A2");
      assertThat(outputTopic.isEmpty()).isTrue();
    }
  }
}
