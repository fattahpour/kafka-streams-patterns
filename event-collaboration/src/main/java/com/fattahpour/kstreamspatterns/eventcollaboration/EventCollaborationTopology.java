package com.fattahpour.kstreamspatterns.eventcollaboration;

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

public final class EventCollaborationTopology {
  private static final String DEFAULT_ALPHA = "pattern.event-collaboration.alpha";
  private static final String DEFAULT_BETA = "pattern.event-collaboration.beta";
  private static final String DEFAULT_JOINED = "pattern.event-collaboration.joined";
  private static final String DEFAULT_LATE = "pattern.event-collaboration.late";

  private EventCollaborationTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<AlphaEvent> alphaSerde = JsonSerdes.serde(AlphaEvent.class);
    Serde<BetaEvent> betaSerde = JsonSerdes.serde(BetaEvent.class);
    Serde<CollaboratedEvent> joinedSerde = JsonSerdes.serde(CollaboratedEvent.class);
    Serde<LateEvent> lateSerde = JsonSerdes.serde(LateEvent.class);
    Serde<CollaborationEnvelope> envelopeSerde = JsonSerdes.serde(CollaborationEnvelope.class);
    Serde<CollaborationOutcome> outcomeSerde = JsonSerdes.serde(CollaborationOutcome.class);
    Serde<CollaborationState> stateSerde = JsonSerdes.serde(CollaborationState.class);

    String alphaTopic = System.getProperty("alpha.topic", DEFAULT_ALPHA);
    String betaTopic = System.getProperty("beta.topic", DEFAULT_BETA);
    String joinedTopic = System.getProperty("joined.topic", DEFAULT_JOINED);
    String lateTopic = System.getProperty("late.topic", DEFAULT_LATE);
    long latenessMs = Long.parseLong(System.getProperty("collaboration.lateness.ms", "5000"));

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(CollaborationTransformer.STORE_NAME),
            Serdes.String(),
            stateSerde));

    KStream<String, CollaborationEnvelope> alphaStream =
        builder
            .stream(alphaTopic, Consumed.with(Serdes.String(), alphaSerde))
            .mapValues(
                event -> new CollaborationEnvelope(SourceType.ALPHA, event.value(), event.correlationId()))
            .selectKey((key, value) -> key);

    KStream<String, CollaborationEnvelope> betaStream =
        builder
            .stream(betaTopic, Consumed.with(Serdes.String(), betaSerde))
            .mapValues(
                event -> new CollaborationEnvelope(SourceType.BETA, event.detail(), event.correlationId()))
            .selectKey((key, value) -> key);

    KStream<String, CollaborationEnvelope> merged = alphaStream.merge(betaStream);

    KStream<String, CollaborationOutcome> outcomes =
        merged.transform(
            new CollaborationTransformer(latenessMs, metrics),
            Named.as("collaboration-transformer"),
            CollaborationTransformer.STORE_NAME);

    KStream<String, CollaboratedEvent> joined =
        outcomes
            .filter((key, value) -> value != null && value.joined() != null)
            .mapValues(CollaborationOutcome::joined);

    KStream<String, LateEvent> late =
        outcomes
            .filter((key, value) -> value != null && value.late() != null)
            .mapValues(CollaborationOutcome::late);

    joined.to(joinedTopic, Produced.with(Serdes.String(), joinedSerde));
    late.to(lateTopic, Produced.with(Serdes.String(), lateSerde));

    return builder.build();
  }
}
