package io.example.kstreamspatterns.aggregatereducecount;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void aggregateReduceCount() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, Long> in =
          testDriver.createInputTopic(
              "input-arc", Serdes.String().serializer(), Serdes.Long().serializer());
      TestOutputTopic<String, Long> sum =
          testDriver.createOutputTopic(
              "sum-arc", Serdes.String().deserializer(), Serdes.Long().deserializer());
      TestOutputTopic<String, Long> max =
          testDriver.createOutputTopic(
              "max-arc", Serdes.String().deserializer(), Serdes.Long().deserializer());
      TestOutputTopic<String, Long> count =
          testDriver.createOutputTopic(
              "count-arc", Serdes.String().deserializer(), Serdes.Long().deserializer());

      in.pipeInput("k", 1L);
      in.pipeInput("k", 2L);
      in.pipeInput("k", 3L);

      List<Long> sums = sum.readValuesToList();
      List<Long> maxes = max.readValuesToList();
      List<Long> counts = count.readValuesToList();

      assertThat(sums).containsExactly(1L, 3L, 6L);
      assertThat(maxes).containsExactly(1L, 2L, 3L);
      assertThat(counts).containsExactly(1L, 2L, 3L);
    }
  }
}
