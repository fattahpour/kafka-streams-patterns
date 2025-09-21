package com.fattahpour.kstreamspatterns.eventcollaboration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EventCollaborationTopologyTest {

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<AlphaEvent> alphaSerde = JsonSerdes.serde(AlphaEvent.class);
  private final Serde<BetaEvent> betaSerde = JsonSerdes.serde(BetaEvent.class);
  private final Serde<CollaboratedEvent> joinedSerde = JsonSerdes.serde(CollaboratedEvent.class);
  private final Serde<LateEvent> lateSerde = JsonSerdes.serde(LateEvent.class);

  @AfterEach
  void clearProps() {
    System.clearProperty("collaboration.lateness.ms");
  }

  @Test
  void joinsAlphaAndBetaWithinLatenessWindow() {
    System.setProperty("collaboration.lateness.ms", "5000");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = EventCollaborationTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "collab-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, AlphaEvent> alphaTopic =
          driver.createInputTopic(
              "pattern.event-collaboration.alpha",
              stringSerde.serializer(),
              alphaSerde.serializer());
      TestInputTopic<String, BetaEvent> betaTopic =
          driver.createInputTopic(
              "pattern.event-collaboration.beta",
              stringSerde.serializer(),
              betaSerde.serializer());
      TestOutputTopic<String, CollaboratedEvent> joinedTopic =
          driver.createOutputTopic(
              "pattern.event-collaboration.joined",
              stringSerde.deserializer(),
              joinedSerde.deserializer());

      alphaTopic.pipeInput("id-1", new AlphaEvent("id-1", "alpha", "corr-1"), 0L);
      betaTopic.pipeInput("id-1", new BetaEvent("id-1", "beta", "corr-1"), 2000L);

      CollaboratedEvent event = joinedTopic.readValue();
      assertThat(event.id()).isEqualTo("id-1");
      assertThat(event.alphaValue()).isEqualTo("alpha");
      assertThat(event.betaValue()).isEqualTo("beta");
      assertThat(metrics.joined()).isEqualTo(1);
      assertThat(metrics.late()).isZero();
    }
  }

  @Test
  void routesLateEventsToLateTopic() {
    System.setProperty("collaboration.lateness.ms", "3000");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = EventCollaborationTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "collab-test-late");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, AlphaEvent> alphaTopic =
          driver.createInputTopic(
              "pattern.event-collaboration.alpha",
              stringSerde.serializer(),
              alphaSerde.serializer());
      TestInputTopic<String, BetaEvent> betaTopic =
          driver.createInputTopic(
              "pattern.event-collaboration.beta",
              stringSerde.serializer(),
              betaSerde.serializer());
      TestOutputTopic<String, LateEvent> lateTopic =
          driver.createOutputTopic(
              "pattern.event-collaboration.late",
              stringSerde.deserializer(),
              lateSerde.deserializer());

      alphaTopic.pipeInput("id-2", new AlphaEvent("id-2", "alpha", "corr-2"), 0L);
      betaTopic.pipeInput("id-2", new BetaEvent("id-2", "beta", "corr-2"), 6000L);

      LateEvent late = lateTopic.readValue();
      assertThat(late.id()).isEqualTo("id-2");
      assertThat(metrics.late()).isEqualTo(1);
      assertThat(metrics.joined()).isZero();
    }
  }
}
