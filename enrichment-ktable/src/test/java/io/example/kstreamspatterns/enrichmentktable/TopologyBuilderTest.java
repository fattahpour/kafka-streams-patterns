package io.example.kstreamspatterns.enrichmentktable;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void enrichesStreamWithTable() {
    try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> productsTopic =
          driver.createInputTopic(
              "products", Serdes.String().serializer(), Serdes.String().serializer());
      TestInputTopic<String, String> ordersTopic =
          driver.createInputTopic(
              "orders", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> outTopic =
          driver.createOutputTopic(
              "enriched-orders", Serdes.String().deserializer(), Serdes.String().deserializer());
      productsTopic.pipeInput("p1", "apple");
      ordersTopic.pipeInput("p1", "5");
      ordersTopic.pipeInput("p2", "3");
      List<KeyValue<String, String>> results = outTopic.readKeyValuesToList();
      assertThat(results)
          .containsExactly(
              KeyValue.pair("p1", "apple:5"), KeyValue.pair("p2", "unknown:3"));
    }
  }
}
