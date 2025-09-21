package com.fattahpour.kstreamspatterns.cqrsprojections;

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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

class CqrsProjectionsTopologyTest {

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<Command> commandSerde = JsonSerdes.serde(Command.class);
  private final Serde<ProjectionEvent> eventSerde = JsonSerdes.serde(ProjectionEvent.class);
  private final Serde<CommandError> errorSerde = JsonSerdes.serde(CommandError.class);

  @Test
  void processesCreateAndUpdateCommands() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = CqrsProjectionsTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cqrs-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, Command> commands =
          driver.createInputTopic(
              "pattern.cqrs.commands",
              stringSerde.serializer(),
              commandSerde.serializer());
      TestOutputTopic<String, ProjectionEvent> events =
          driver.createOutputTopic(
              "pattern.cqrs.events",
              stringSerde.deserializer(),
              eventSerde.deserializer());
      KeyValueStore<String, ProjectionState> store = driver.getKeyValueStore(CommandProcessor.STORE_NAME);

      commands.pipeInput("agg-1", new Command("agg-1", "CREATE", "open"));
      commands.pipeInput("agg-1", new Command("agg-1", "UPDATE", "closed"));

      List<TestRecord<String, ProjectionEvent>> records = events.readRecordsToList();
      assertThat(records).hasSize(2);
      ProjectionState state = store.get("agg-1");
      assertThat(state.payload()).isEqualTo("closed");
      assertThat(state.version()).isEqualTo(2);
      assertThat(metrics.accepted()).isEqualTo(2);
      assertThat(metrics.rejected()).isZero();
    }
  }

  @Test
  void routesUpdateWithoutStateToDlq() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = CqrsProjectionsTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cqrs-test-dlq");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, Command> commands =
          driver.createInputTopic(
              "pattern.cqrs.commands",
              stringSerde.serializer(),
              commandSerde.serializer());
      TestOutputTopic<String, CommandError> dlq =
          driver.createOutputTopic(
              "pattern.cqrs.dlq",
              stringSerde.deserializer(),
              errorSerde.deserializer());

      commands.pipeInput("agg-2", new Command("agg-2", "UPDATE", "value"));

      CommandError error = dlq.readValue();
      assertThat(error.reason()).isEqualTo("aggregate-missing");
      assertThat(metrics.rejected()).isEqualTo(1);
    }
  }
}
