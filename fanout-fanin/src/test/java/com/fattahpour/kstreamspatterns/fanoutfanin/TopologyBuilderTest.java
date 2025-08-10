package com.fattahpour.kstreamspatterns.fanoutfanin;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void branchesAndMerges() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> input =
          testDriver.createInputTopic(
              "fanout-input", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> output =
          testDriver.createOutputTopic(
              "fanout-output", Serdes.String().deserializer(), Serdes.String().deserializer());
      input.pipeInput("k1", "1");
      input.pipeInput("k2", "2");
      input.pipeInput("k3", "3");
      assertThat(output.readValuesToList()).containsExactly("3", "4", "9");
    }
  }
}
