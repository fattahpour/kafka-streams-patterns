package io.example.kstreamspatterns.statelesstransforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void mapFilterFlatMap() {
    java.util.Properties props = new java.util.Properties();
    props.put(
        org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    props.put(
        org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
      TestInputTopic<String, String> in =
          testDriver.createInputTopic("input-stateless", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> out =
          testDriver.createOutputTopic("output-stateless", Serdes.String().deserializer(), Serdes.String().deserializer());
      in.pipeInput("k1", "hello world");
      in.pipeInput("k2", "ignore me");
      List<String> values = out.readValuesToList();
      assertThat(values).containsExactly("HELLO", "WORLD");
    }
  }
}
