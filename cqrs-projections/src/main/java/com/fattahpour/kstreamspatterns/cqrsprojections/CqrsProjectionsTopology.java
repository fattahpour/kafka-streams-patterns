package com.fattahpour.kstreamspatterns.cqrsprojections;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public final class CqrsProjectionsTopology {
  private static final String DEFAULT_COMMANDS = "pattern.cqrs.commands";
  private static final String DEFAULT_EVENTS = "pattern.cqrs.events";
  private static final String DEFAULT_DLQ = "pattern.cqrs.dlq";

  private CqrsProjectionsTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<Command> commandSerde = JsonSerdes.serde(Command.class);
    Serde<ProjectionEvent> eventSerde = JsonSerdes.serde(ProjectionEvent.class);
    Serde<CommandError> errorSerde = JsonSerdes.serde(CommandError.class);
    Serde<ProjectionOutcome> outcomeSerde = JsonSerdes.serde(ProjectionOutcome.class);
    Serde<ProjectionState> stateSerde = JsonSerdes.serde(ProjectionState.class);

    String commandsTopic = System.getProperty("commands.topic", DEFAULT_COMMANDS);
    String eventsTopic = System.getProperty("events.topic", DEFAULT_EVENTS);
    String dlqTopic = System.getProperty("dlq.topic", DEFAULT_DLQ);

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(CommandProcessor.STORE_NAME),
            Serdes.String(),
            stateSerde));

    KStream<String, Command> commands =
        builder.stream(commandsTopic, Consumed.with(Serdes.String(), commandSerde));

    KStream<String, ProjectionOutcome> outcomes =
        commands.transform(
            new CommandProcessor(metrics),
            Named.as("cqrs-command-processor"),
            CommandProcessor.STORE_NAME);

    outcomes
        .filter((key, value) -> value != null && value.event() != null)
        .mapValues(ProjectionOutcome::event)
        .to(eventsTopic, Produced.with(Serdes.String(), eventSerde));

    outcomes
        .filter((key, value) -> value != null && value.error() != null)
        .mapValues(ProjectionOutcome::error)
        .to(dlqTopic, Produced.with(Serdes.String(), errorSerde));

    return builder.build();
  }
}
