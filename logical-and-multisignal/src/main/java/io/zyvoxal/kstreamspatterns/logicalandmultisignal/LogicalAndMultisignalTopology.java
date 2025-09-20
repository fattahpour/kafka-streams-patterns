package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public final class LogicalAndMultisignalTopology {
  private static final String DEFAULT_INPUT = "pattern.logical-and-multisignal.in";
  private static final String DEFAULT_OUTPUT = "pattern.logical-and-multisignal.out";
  private static final String DEFAULT_EXPIRED = "pattern.logical-and-multisignal.expired";

  private LogicalAndMultisignalTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<SignalEnvelope> envelopeSerde = JsonSerdes.serde(SignalEnvelope.class);
    Serde<CorrelatedSignal> correlatedSerde = JsonSerdes.serde(CorrelatedSignal.class);
    Serde<ExpiredCorrelation> expiredSerde = JsonSerdes.serde(ExpiredCorrelation.class);
    Serde<CorrelationState> stateSerde = JsonSerdes.serde(CorrelationState.class);

    String input = System.getProperty("input.topic", DEFAULT_INPUT);
    String output = System.getProperty("output.topic", DEFAULT_OUTPUT);
    String expired = System.getProperty("expired.topic", DEFAULT_EXPIRED);
    Duration window =
        Duration.ofMillis(Long.parseLong(System.getProperty("correlation.window.ms", "60000")));

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(SignalCorrelationTransformer.STORE_NAME),
            Serdes.String(),
            stateSerde));

    KStream<String, SignalEnvelope> source =
        builder.stream(input, Consumed.with(Serdes.String(), envelopeSerde));

    KStream<String, CorrelationResult> results =
        source.transform(
            new SignalCorrelationTransformer(window, metrics),
            Named.as("signal-correlation"),
            SignalCorrelationTransformer.STORE_NAME);

    KStream<String, CorrelatedSignal> correlated =
        results
            .filter((key, value) -> value != null && value.correlatedSignal() != null)
            .mapValues(CorrelationResult::correlatedSignal);

    KStream<String, ExpiredCorrelation> expiredStream =
        results
            .filter((key, value) -> value != null && value.expiredCorrelation() != null)
            .mapValues(CorrelationResult::expiredCorrelation);

    correlated.to(output, Produced.with(Serdes.String(), correlatedSerde));
    expiredStream.to(expired, Produced.with(Serdes.String(), expiredSerde));

    return builder.build();
  }
}
