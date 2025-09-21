package com.fattahpour.kstreamspatterns.pipelinestrangler;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public final class PipelineStranglerTopology {
  private static final String DEFAULT_INPUT = "pattern.pipeline-strangler.input";
  private static final String DEFAULT_LEGACY = "pattern.pipeline-strangler.legacy";
  private static final String DEFAULT_MODERN = "pattern.pipeline-strangler.modern";

  private PipelineStranglerTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<PipelineEvent> eventSerde = JsonSerdes.serde(PipelineEvent.class);

    String input = System.getProperty("input.topic", DEFAULT_INPUT);
    String legacyTopic = System.getProperty("legacy.topic", DEFAULT_LEGACY);
    String modernTopic = System.getProperty("modern.topic", DEFAULT_MODERN);
    RoutingMode mode = RoutingMode.from(System.getProperty("strangler.mode", "dual"));

    KStream<String, PipelineEvent> stream =
        builder
            .stream(input, Consumed.with(Serdes.String(), eventSerde))
            .peek((key, value) -> metrics.markIngress());

    switch (mode) {
      case LEGACY ->
          stream
              .peek((key, value) -> metrics.markLegacy())
              .to(legacyTopic);
      case MODERN ->
          stream
              .peek((key, value) -> metrics.markModern())
              .to(modernTopic);
      case DUAL -> {
        stream.peek((key, value) -> metrics.markLegacy()).to(legacyTopic);
        stream.peek((key, value) -> metrics.markModern()).to(modernTopic);
      }
    }

    return builder.build();
  }
}
