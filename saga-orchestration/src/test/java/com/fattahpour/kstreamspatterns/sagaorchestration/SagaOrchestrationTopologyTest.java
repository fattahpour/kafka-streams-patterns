package com.fattahpour.kstreamspatterns.sagaorchestration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

class SagaOrchestrationTopologyTest {

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<OrderCommand> commandSerde = JsonSerdes.serde(OrderCommand.class);
  private final Serde<SagaEvent> eventSerde = JsonSerdes.serde(SagaEvent.class);
  private final Serde<CompensationEvent> compensationSerde = JsonSerdes.serde(CompensationEvent.class);
  private final Serde<OrderError> errorSerde = JsonSerdes.serde(OrderError.class);

  @Test
  void orchestratesHappyPathOrder() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = SagaOrchestrationTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "saga-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, OrderCommand> orders =
          driver.createInputTopic(
              "pattern.saga.orders",
              stringSerde.serializer(),
              commandSerde.serializer());
      TestOutputTopic<String, SagaEvent> events =
          driver.createOutputTopic(
              "pattern.saga.events",
              stringSerde.deserializer(),
              eventSerde.deserializer());
      TestOutputTopic<String, CompensationEvent> compensations =
          driver.createOutputTopic(
              "pattern.saga.compensations",
              stringSerde.deserializer(),
              compensationSerde.deserializer());

      orders.pipeInput("order-1", new OrderCommand("order-1", false, "payload"));

      List<TestRecord<String, SagaEvent>> eventRecords = events.readRecordsToList();
      assertThat(eventRecords).extracting(r -> r.value().type())
          .containsExactly(
              "INVENTORY_RESERVED",
              "PAYMENT_COMPLETED",
              "ORDER_COMPLETED");
      assertThat(compensations.isEmpty()).isTrue();
      assertThat(metrics.started()).isEqualTo(1);
      assertThat(metrics.completed()).isEqualTo(1);
      assertThat(metrics.compensated()).isZero();
    }
  }

  @Test
  void triggersCompensationOnFailure() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = SagaOrchestrationTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "saga-test-failure");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, OrderCommand> orders =
          driver.createInputTopic(
              "pattern.saga.orders",
              stringSerde.serializer(),
              commandSerde.serializer());
      TestOutputTopic<String, SagaEvent> events =
          driver.createOutputTopic(
              "pattern.saga.events",
              stringSerde.deserializer(),
              eventSerde.deserializer());
      TestOutputTopic<String, CompensationEvent> compensations =
          driver.createOutputTopic(
              "pattern.saga.compensations",
              stringSerde.deserializer(),
              compensationSerde.deserializer());

      orders.pipeInput("order-2", new OrderCommand("order-2", true, "payload"));

      List<TestRecord<String, SagaEvent>> eventRecords = events.readRecordsToList();
      assertThat(eventRecords).extracting(r -> r.value().type())
          .containsExactly(
              "INVENTORY_RESERVED",
              "PAYMENT_FAILED",
              "ORDER_FAILED");
      CompensationEvent compensation = compensations.readValue();
      assertThat(compensation.type()).isEqualTo("RELEASE_INVENTORY");
      assertThat(metrics.compensated()).isEqualTo(1);
    }
  }
}
