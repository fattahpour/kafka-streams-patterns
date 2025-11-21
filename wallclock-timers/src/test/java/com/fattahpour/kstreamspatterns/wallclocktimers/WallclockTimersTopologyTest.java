package com.fattahpour.kstreamspatterns.wallclocktimers;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class WallclockTimersTopologyTest {

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<ScheduleCommand> commandSerde = JsonSerdes.serde(ScheduleCommand.class);
    private final Serde<TimerFiredEvent> firedSerde = JsonSerdes.serde(TimerFiredEvent.class);
    private final Serde<TimerError> errorSerde = JsonSerdes.serde(TimerError.class);

    @Test
    void firesTimerOnWallClockAdvance() {
        PatternMetrics metrics = new PatternMetrics();
        Topology topology = WallclockTimersTopology.build(metrics);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wallclock-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, ScheduleCommand> commands =
                    driver.createInputTopic(
                            "pattern.wallclock-timers.commands",
                            stringSerde.serializer(),
                            commandSerde.serializer());
            TestOutputTopic<String, TimerFiredEvent> fired =
                    driver.createOutputTopic(
                            "pattern.wallclock-timers.fired",
                            stringSerde.deserializer(),
                            firedSerde.deserializer());

            ScheduleCommand command = new ScheduleCommand("timer-1", 1000L, "reminder", "corr-1");
            commands.pipeInput("timer-1", command, 0L);

            driver.advanceWallClockTime(Duration.ofMillis(1000));

            List<TimerFiredEvent> firedEvents = fired.readValuesToList();
            assertThat(firedEvents).hasSize(1);
            TimerFiredEvent event = firedEvents.get(0);
            assertThat(event.id()).isEqualTo("timer-1");
            assertThat(event.payload()).isEqualTo("reminder");
            assertThat(metrics.fired()).isEqualTo(1);
        }
    }

    @Test
    void routesInvalidCommandsToDlq() {
        PatternMetrics metrics = new PatternMetrics();
        Topology topology = WallclockTimersTopology.build(metrics);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wallclock-test-invalid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, ScheduleCommand> commands =
                    driver.createInputTopic(
                            "pattern.wallclock-timers.commands",
                            stringSerde.serializer(),
                            commandSerde.serializer());
            TestOutputTopic<String, TimerError> dlq =
                    driver.createOutputTopic(
                            "pattern.wallclock-timers.dlq",
                            stringSerde.deserializer(),
                            errorSerde.deserializer());

            commands.pipeInput("bad", new ScheduleCommand(null, -1L, "payload", null));

            TimerError error = dlq.readValue();
            assertThat(error.reason()).isEqualTo("invalid-command");
            assertThat(metrics.invalid()).isEqualTo(1);
            assertThat(dlq.isEmpty()).isTrue();
        }
    }
}
