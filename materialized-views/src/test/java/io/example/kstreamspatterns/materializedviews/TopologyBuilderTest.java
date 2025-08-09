package io.example.kstreamspatterns.materializedviews;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void countsAndStoresValues() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
      TestInputTopic<String, String> input =
          testDriver.createInputTopic(
              "input-materialized",
              Serdes.String().serializer(),
              Serdes.String().serializer());
      TestOutputTopic<String, Long> output =
          testDriver.createOutputTopic(
              "output-materialized",
              Serdes.String().deserializer(),
              Serdes.Long().deserializer());

      input.pipeInput("a", "1");
      input.pipeInput("a", "2");

      assertThat(output.readValue()).isEqualTo(1L);
      assertThat(output.readValue()).isEqualTo(2L);

      KeyValueStore<String, Long> store = testDriver.getKeyValueStore("counts-store");
      assertThat(store.get("a")).isEqualTo(2L);
    }
  }
}
