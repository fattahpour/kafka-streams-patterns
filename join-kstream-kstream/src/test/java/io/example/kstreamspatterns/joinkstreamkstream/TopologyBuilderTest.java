package io.example.kstreamspatterns.joinkstreamkstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void joinsWithinWindow() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> left =
          testDriver.createInputTopic(
              "left-join", Serdes.String().serializer(), Serdes.String().serializer());
      TestInputTopic<String, String> right =
          testDriver.createInputTopic(
              "right-join", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> output =
          testDriver.createOutputTopic(
              "joined", Serdes.String().deserializer(), Serdes.String().deserializer());
      left.pipeInput("k1", "L1", 0L);
      right.pipeInput("k1", "R1", 1000L);
      assertThat(output.readValuesToList()).containsExactly("L1:R1");
    }
  }

  @Test
  void noJoinWhenOutsideWindow() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> left =
          testDriver.createInputTopic(
              "left-join", Serdes.String().serializer(), Serdes.String().serializer());
      TestInputTopic<String, String> right =
          testDriver.createInputTopic(
              "right-join", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> output =
          testDriver.createOutputTopic(
              "joined", Serdes.String().deserializer(), Serdes.String().deserializer());
      left.pipeInput("k2", "L2", 0L);
      right.pipeInput("k2", "R2", Duration.ofMinutes(6).toMillis());
      assertThat(output.isEmpty()).isTrue();
    }
  }
}
