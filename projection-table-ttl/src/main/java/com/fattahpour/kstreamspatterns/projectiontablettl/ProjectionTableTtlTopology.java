package com.fattahpour.kstreamspatterns.projectiontablettl;

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

public final class ProjectionTableTtlTopology {
  private static final String DEFAULT_UPDATES = "pattern.projection-table-ttl.updates";
  private static final String DEFAULT_VIEW = "pattern.projection-table-ttl.view";
  private static final String DEFAULT_EXPIRED = "pattern.projection-table-ttl.expired";

  private ProjectionTableTtlTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<ProjectionUpdate> updateSerde = JsonSerdes.serde(ProjectionUpdate.class);
    Serde<ProjectionView> viewSerde = JsonSerdes.serde(ProjectionView.class);
    Serde<ExpiredProjection> expiredSerde = JsonSerdes.serde(ExpiredProjection.class);
    Serde<ProjectionResult> resultSerde = JsonSerdes.serde(ProjectionResult.class);
    Serde<ProjectionState> stateSerde = JsonSerdes.serde(ProjectionState.class);

    String updatesTopic = System.getProperty("updates.topic", DEFAULT_UPDATES);
    String viewTopic = System.getProperty("view.topic", DEFAULT_VIEW);
    String expiredTopic = System.getProperty("expired.topic", DEFAULT_EXPIRED);
    long ttlMs = Long.parseLong(System.getProperty("projection.ttl.ms", "60000"));

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(ProjectionTableTransformer.STORE_NAME),
            Serdes.String(),
            stateSerde));

    KStream<String, ProjectionUpdate> updates =
        builder.stream(updatesTopic, Consumed.with(Serdes.String(), updateSerde));

    KStream<String, ProjectionResult> results =
        updates.transform(
            new ProjectionTableTransformer(ttlMs, metrics),
            Named.as("projection-table-transform"),
            ProjectionTableTransformer.STORE_NAME);

    results
        .filter((key, value) -> value != null && value.view() != null)
        .mapValues(ProjectionResult::view)
        .to(viewTopic, Produced.with(Serdes.String(), viewSerde));

    results
        .filter((key, value) -> value != null && value.expired() != null)
        .mapValues(ProjectionResult::expired)
        .to(expiredTopic, Produced.with(Serdes.String(), expiredSerde));

    return builder.build();
  }
}
