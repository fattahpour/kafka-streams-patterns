package com.fattahpour.kstreamspatterns.deduplication;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void dropsDuplicatesWithinWindow() {
    try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> input =
          driver.createInputTopic(
              "input-dedup",
              Serdes.String().serializer(),
              Serdes.String().serializer());
      TestOutputTopic<String, String> output =
          driver.createOutputTopic(
              "output-dedup",
              Serdes.String().deserializer(),
              Serdes.String().deserializer());
      input.pipeInput("k1", "a", Instant.ofEpochMilli(0));
      input.pipeInput("k1", "b", Instant.ofEpochMilli(1000));
      input.pipeInput("k1", "c", Instant.ofEpochMilli(11 * 60 * 1000L));
      assertThat(output.readValuesToList()).containsExactly("a", "c");
    }
  }
}
