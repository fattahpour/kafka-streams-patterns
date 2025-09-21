package com.fattahpour.kstreamspatterns.projectiontablettl;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ProjectionTableTtlTopologyTest {

  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<ProjectionUpdate> updateSerde = JsonSerdes.serde(ProjectionUpdate.class);
  private final Serde<ProjectionView> viewSerde = JsonSerdes.serde(ProjectionView.class);
  private final Serde<ExpiredProjection> expiredSerde = JsonSerdes.serde(ExpiredProjection.class);

  @AfterEach
  void clearProps() {
    System.clearProperty("projection.ttl.ms");
  }

  @Test
  void appliesVersionedUpserts() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = ProjectionTableTtlTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "projection-ttl-upsert");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, ProjectionUpdate> updates =
          driver.createInputTopic(
              "pattern.projection-table-ttl.updates",
              stringSerde.serializer(),
              updateSerde.serializer());
      TestOutputTopic<String, ProjectionView> viewTopic =
          driver.createOutputTopic(
              "pattern.projection-table-ttl.view",
              stringSerde.deserializer(),
              viewSerde.deserializer());

      updates.pipeInput("id-1", new ProjectionUpdate("id-1", 1L, "open", 0L));
      updates.pipeInput("id-1", new ProjectionUpdate("id-1", 2L, "closed", 5L));
      updates.pipeInput("id-1", new ProjectionUpdate("id-1", 1L, "ignored", 10L));

      List<ProjectionView> views = viewTopic.readValuesToList();
      assertThat(views).hasSize(2);
      ProjectionView latest = views.get(1);
      assertThat(latest.version()).isEqualTo(2L);
      assertThat(latest.payload()).isEqualTo("closed");
      assertThat(metrics.upserts()).isEqualTo(2);
      assertThat(metrics.skipped()).isEqualTo(1);
    }
  }

  @Test
  void expiresEntriesAfterTtl() {
    System.setProperty("projection.ttl.ms", "1000");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = ProjectionTableTtlTopology.build(metrics);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "projection-ttl-expire");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
      TestInputTopic<String, ProjectionUpdate> updates =
          driver.createInputTopic(
              "pattern.projection-table-ttl.updates",
              stringSerde.serializer(),
              updateSerde.serializer());
      TestOutputTopic<String, ExpiredProjection> expiredTopic =
          driver.createOutputTopic(
              "pattern.projection-table-ttl.expired",
              stringSerde.deserializer(),
              expiredSerde.deserializer());

      updates.pipeInput("id-2", new ProjectionUpdate("id-2", 1L, "value", 0L));

      driver.advanceWallClockTime(Duration.ofMillis(1500));

      ExpiredProjection expired = expiredTopic.readValue();
      assertThat(expired.id()).isEqualTo("id-2");
      assertThat(metrics.expired()).isEqualTo(1);
    }
  }
}
