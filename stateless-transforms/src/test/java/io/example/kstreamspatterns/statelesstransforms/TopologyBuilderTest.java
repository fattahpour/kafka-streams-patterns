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
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
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
