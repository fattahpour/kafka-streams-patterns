package com.fattahpour.kstreamspatterns.suppression;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void emitsFinalResultsOnly() {
    try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> input =
          driver.createInputTopic(
              "input-suppression",
              Serdes.String().serializer(),
              Serdes.String().serializer());
      TestOutputTopic<String, Long> output =
          driver.createOutputTopic(
              "output-suppression",
              Serdes.String().deserializer(),
              Serdes.Long().deserializer());
      input.pipeInput("k1", "a", Instant.ofEpochMilli(0));
      input.pipeInput("k1", "b", Instant.ofEpochMilli(1000));
      assertThat(output.isEmpty()).isTrue();
      input.pipeInput("k2", "x", Instant.ofEpochMilli(Duration.ofMinutes(1).toMillis() + 1));
      assertThat(output.readKeyValuesToList()).containsExactly(KeyValue.pair("k1", 2L));
    }
  }
}
