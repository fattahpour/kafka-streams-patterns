package io.example.kstreamspatterns.retrydlq;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

public class TopologyBuilderTest {
  @Test
  void routesThroughRetriesAndDlq() {
    System.setProperty("input.topic", "in");
    System.setProperty("retry1.topic", "r1");
    System.setProperty("retry2.topic", "r2");
    System.setProperty("dlq.topic", "dead");
    System.setProperty("output.topic", "out");

    try (TopologyTestDriver testDriver = new TopologyTestDriver(TopologyBuilder.build())) {
      var inputFactory =
          new org.apache.kafka.streams.test.TestRecordFactory<>(
              Serdes.String().serializer(), Serdes.String().serializer());

      // success
      testDriver.pipeInput(inputFactory.create("in", "k1", "ok"));
      ProducerRecord<String, String> success =
          testDriver.readOutput("out", Serdes.String().deserializer(), Serdes.String().deserializer());
      assertThat(success.value()).isEqualTo("OK");

      // fail -> retry1
      testDriver.pipeInput(inputFactory.create("in", "k2", "fail"));
      ProducerRecord<String, String> retry1 =
          testDriver.readOutput("r1", Serdes.String().deserializer(), Serdes.String().deserializer());
      assertThat(new String(retry1.headers().lastHeader("attempt").value(), StandardCharsets.UTF_8))
          .isEqualTo("1");

      // retry1 -> retry2
      RecordHeaders h1 = new RecordHeaders();
      h1.add("attempt", "1".getBytes(StandardCharsets.UTF_8));
      testDriver.pipeInput(new TestRecord<>("k2", "fail", h1, null, "r1"));
      ProducerRecord<String, String> retry2 =
          testDriver.readOutput("r2", Serdes.String().deserializer(), Serdes.String().deserializer());
      assertThat(new String(retry2.headers().lastHeader("attempt").value(), StandardCharsets.UTF_8))
          .isEqualTo("2");

      // retry2 -> dlq
      RecordHeaders h2 = new RecordHeaders();
      h2.add("attempt", "2".getBytes(StandardCharsets.UTF_8));
      testDriver.pipeInput(new TestRecord<>("k2", "fail", h2, null, "r2"));
      ProducerRecord<String, String> dlq =
          testDriver.readOutput("dead", Serdes.String().deserializer(), Serdes.String().deserializer());
      assertThat(new String(dlq.headers().lastHeader("attempt").value(), StandardCharsets.UTF_8))
          .isEqualTo("3");
      assertThat(new String(dlq.headers().lastHeader("error").value(), StandardCharsets.UTF_8))
          .isEqualTo("boom");
    }
  }
}
