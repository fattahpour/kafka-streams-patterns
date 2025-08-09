package io.example.kstreamspatterns.lateearlydata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void emitsEarlyAndFinalCountsAndDropsLateData() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "late-early-data-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
      TestInputTopic<String, Long> input =
          driver.createInputTopic(
              "late-early-input",
              Serdes.String().serializer(),
              Serdes.Long().serializer());
      TestOutputTopic<String, Long> output =
          driver.createOutputTopic(
              "late-early-output",
              Serdes.String().deserializer(),
              Serdes.Long().deserializer());

      input.pipeInput("k", 1L, 0L);
      input.pipeInput("advance", 0L, 2000L);

      List<TestRecord<String, Long>> first = output.readRecordsToList();
      assertThat(first).anyMatch(r -> r.key().equals("k@0") && r.value() == 1L);

      input.pipeInput("k", 1L, 3000L);
      input.pipeInput("advance2", 0L, 11000L);

      List<TestRecord<String, Long>> second = output.readRecordsToList();
      assertThat(second).anyMatch(r -> r.key().equals("k@0") && r.value() == 2L);

      input.pipeInput("k", 1L, 12000L);
      assertThat(output.isEmpty()).isTrue();
    }
  }
}
