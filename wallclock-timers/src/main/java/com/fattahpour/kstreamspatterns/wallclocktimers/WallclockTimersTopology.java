package com.fattahpour.kstreamspatterns.wallclocktimers;

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

public final class WallclockTimersTopology {
  static final String STORE_NAME = "wallclock-timer-store";
  private static final String DEFAULT_COMMANDS = "pattern.wallclock-timers.commands";
  private static final String DEFAULT_FIRED = "pattern.wallclock-timers.fired";
  private static final String DEFAULT_DLQ = "pattern.wallclock-timers.dlq";

  private WallclockTimersTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<ScheduleCommand> commandSerde = JsonSerdes.serde(ScheduleCommand.class);
    Serde<TimerFiredEvent> firedSerde = JsonSerdes.serde(TimerFiredEvent.class);
    Serde<TimerError> errorSerde = JsonSerdes.serde(TimerError.class);
    Serde<ScheduledTimer> stateSerde = JsonSerdes.serde(ScheduledTimer.class);

    String commandsTopic = System.getProperty("commands.topic", DEFAULT_COMMANDS);
    String firedTopic = System.getProperty("fired.topic", DEFAULT_FIRED);
    String dlqTopic = System.getProperty("dlq.topic", DEFAULT_DLQ);
    long checkInterval = Long.parseLong(System.getProperty("timer.check.ms", "1000"));
    long grace = Long.parseLong(System.getProperty("timer.grace.ms", "0"));

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(STORE_NAME),
            Serdes.String(),
            stateSerde));

    KStream<String, ScheduleCommand> source =
        builder.stream(commandsTopic, Consumed.with(Serdes.String(), commandSerde));

    KStream<String, ScheduleCommand>[] branches =
        source.branch(
            (key, value) -> isValid(value),
            (key, value) -> true);

    KStream<String, ScheduleCommand> valid = branches[0];
    KStream<String, ScheduleCommand> invalid = branches[1];

    invalid
        .mapValues(v -> new TimerError(v != null ? v.id() : null, "invalid-command"))
        .peek((key, value) -> metrics.markInvalid())
        .to(dlqTopic, Produced.with(Serdes.String(), errorSerde));

    KStream<String, TimerFiredEvent> fired =
        valid.transform(
            new SchedulingTransformer(checkInterval, grace, metrics),
            Named.as("wallclock-scheduler"),
            STORE_NAME);

    fired
        .filter((key, value) -> value != null)
        .to(firedTopic, Produced.with(Serdes.String(), firedSerde));

    return builder.build();
  }

  private static boolean isValid(ScheduleCommand command) {
    return command != null && command.id() != null && !command.id().isBlank() && command.dueAt() >= 0;
  }
}
