package io.example.kstreamspatterns.branchroute;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void routesToEvenAndOdd() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> input =
          testDriver.createInputTopic(
              "input-branch", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> even =
          testDriver.createOutputTopic(
              "even-branch", Serdes.String().deserializer(), Serdes.String().deserializer());
      TestOutputTopic<String, String> odd =
          testDriver.createOutputTopic(
              "odd-branch", Serdes.String().deserializer(), Serdes.String().deserializer());
      input.pipeInput("k1", "1");
      input.pipeInput("k2", "2");
      input.pipeInput("k3", "3");
      assertThat(even.readValuesToList()).containsExactly("2");
      assertThat(odd.readValuesToList()).containsExactly("1", "3");
    }
  }
}
