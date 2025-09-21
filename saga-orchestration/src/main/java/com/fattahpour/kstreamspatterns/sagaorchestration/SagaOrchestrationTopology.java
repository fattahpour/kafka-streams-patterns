package com.fattahpour.kstreamspatterns.sagaorchestration;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public final class SagaOrchestrationTopology {
  private static final String DEFAULT_ORDERS = "pattern.saga.orders";
  private static final String DEFAULT_EVENTS = "pattern.saga.events";
  private static final String DEFAULT_COMPENSATIONS = "pattern.saga.compensations";
  private static final String DEFAULT_DLQ = "pattern.saga.dlq";

  private SagaOrchestrationTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<OrderCommand> commandSerde = JsonSerdes.serde(OrderCommand.class);
    Serde<SagaEvent> eventSerde = JsonSerdes.serde(SagaEvent.class);
    Serde<CompensationEvent> compensationSerde = JsonSerdes.serde(CompensationEvent.class);
    Serde<OrderError> errorSerde = JsonSerdes.serde(OrderError.class);
    Serde<SagaOutcome> outcomeSerde = JsonSerdes.serde(SagaOutcome.class);

    String ordersTopic = System.getProperty("orders.topic", DEFAULT_ORDERS);
    String eventsTopic = System.getProperty("events.topic", DEFAULT_EVENTS);
    String compensationsTopic = System.getProperty("compensations.topic", DEFAULT_COMPENSATIONS);
    String dlqTopic = System.getProperty("dlq.topic", DEFAULT_DLQ);

    KStream<String, OrderCommand> orders =
        builder.stream(ordersTopic, Consumed.with(Serdes.String(), commandSerde));

    KStream<String, OrderCommand>[] branches =
        orders.branch(SagaOrchestrationTopology::isValid, (key, value) -> true);

    KStream<String, OrderCommand> valid = branches[0];
    KStream<String, OrderCommand> invalid = branches[1];

    invalid
        .mapValues(value -> new OrderError(value != null ? value.orderId() : null, "invalid-order"))
        .peek((key, value) -> metrics.markRejected())
        .to(dlqTopic, Produced.with(Serdes.String(), errorSerde));

    KStream<String, SagaOutcome> outcomes =
        valid.transformValues(new SagaTransformer(metrics), Named.as("saga-transformer"));

    KStream<String, SagaEvent> events =
        outcomes.flatMapValues(
            outcome -> outcome != null && outcome.events() != null ? outcome.events() : Collections.emptyList());

    events.to(eventsTopic, Produced.with(Serdes.String(), eventSerde));

    outcomes
        .filter((key, value) -> value != null && value.compensation() != null)
        .mapValues(SagaOutcome::compensation)
        .to(compensationsTopic, Produced.with(Serdes.String(), compensationSerde));

    return builder.build();
  }

  private static boolean isValid(String key, OrderCommand command) {
    return command != null && command.orderId() != null && !command.orderId().isBlank();
  }
}
