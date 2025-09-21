package com.fattahpour.kstreamspatterns.eventsplitter;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public final class EventSplitterTopology {
  private static final String DEFAULT_INPUT = "pattern.event-splitter.in";
  private static final String DEFAULT_CHILDREN = "pattern.event-splitter.children";
  private static final String DEFAULT_DLQ = "pattern.event-splitter.dlq";

  private EventSplitterTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<CompositeEvent> compositeSerde = JsonSerdes.serde(CompositeEvent.class);
    Serde<ChildEvent> childSerde = JsonSerdes.serde(ChildEvent.class);
    Serde<SplitterError> errorSerde = JsonSerdes.serde(SplitterError.class);

    String input = System.getProperty("input.topic", DEFAULT_INPUT);
    String children = System.getProperty("children.topic", DEFAULT_CHILDREN);
    String dlq = System.getProperty("dlq.topic", DEFAULT_DLQ);

    KStream<String, CompositeEvent> source =
        builder.stream(input, Consumed.with(Serdes.String(), compositeSerde));

    KStream<String, CompositeEvent>[] branches =
        source.branch(EventSplitterTopology::isValid, (key, value) -> true);

    KStream<String, CompositeEvent> valid = branches[0];
    KStream<String, CompositeEvent> invalid = branches[1];

    invalid
        .mapValues(value -> new SplitterError(value != null ? value.id() : null, "invalid-envelope"))
        .peek((key, value) -> metrics.markInvalid())
        .to(dlq, Produced.with(Serdes.String(), errorSerde));

    KStream<String, ChildEvent> childrenStream =
        valid
            .peek((key, value) -> metrics.markSplit())
            .flatMap(
                (key, value) -> {
                  List<String> fragments = value.fragments();
                  int count = fragments.size();
                  metrics.markFragments(count);
                  List<org.apache.kafka.streams.KeyValue<String, ChildEvent>> results = new ArrayList<>(count);
                  for (int i = 0; i < count; i++) {
                    String fragment = fragments.get(i);
                    String childId = value.id() + "-" + i;
                    ChildEvent child =
                        new ChildEvent(childId, value.id(), i, fragment, value.correlationId());
                    results.add(org.apache.kafka.streams.KeyValue.pair(childId, child));
                  }
                  return results;
                })
            .transformValues(new LineageHeaderTransformer(), Named.as("lineage-headers"));

    childrenStream.to(children, Produced.with(Serdes.String(), childSerde));

    return builder.build();
  }

  private static boolean isValid(String key, CompositeEvent event) {
    return event != null
        && event.id() != null
        && !event.id().isBlank()
        && event.fragments() != null
        && !event.fragments().isEmpty();
  }
}
