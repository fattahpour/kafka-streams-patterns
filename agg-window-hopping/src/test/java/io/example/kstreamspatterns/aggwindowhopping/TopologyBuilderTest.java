package io.example.kstreamspatterns.aggwindowhopping;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void countsPerHoppingWindow() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> input =
          testDriver.createInputTopic(
              "input", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> output =
          testDriver.createOutputTopic(
              "hopping-count", Serdes.String().deserializer(), Serdes.String().deserializer());
      input.pipeInput("k1", "v1", 0L);
      input.pipeInput("k1", "v2", 30000L);
      input.pipeInput("k1", "v3", 60000L);
      assertThat(output.readKeyValuesToList())
          .containsExactly(
              KeyValue.pair("k1@0", "1"),
              KeyValue.pair("k1@0", "2"),
              KeyValue.pair("k1@30000", "1"),
              KeyValue.pair("k1@60000", "1"),
              KeyValue.pair("k1@30000", "2"));
    }
  }
}
