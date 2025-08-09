package io.example.kstreamspatterns.rekeyrepartition;

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
  void rekeysAndRepartitions() {
    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      TestInputTopic<String, String> in =
          testDriver.createInputTopic(
              "input-rekey", Serdes.String().serializer(), Serdes.String().serializer());
      TestOutputTopic<String, String> out =
          testDriver.createOutputTopic(
              "output-rekey", Serdes.String().deserializer(), Serdes.String().deserializer());
      in.pipeInput(null, "u1:a");
      in.pipeInput(null, "u1:b");
      List<KeyValue<String, String>> results = out.readKeyValuesToList();
      assertThat(results)
          .containsExactly(KeyValue.pair("u1", "a"), KeyValue.pair("u1", "b"));
    }
  }
}
