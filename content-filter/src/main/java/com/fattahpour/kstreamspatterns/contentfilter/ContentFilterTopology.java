package com.fattahpour.kstreamspatterns.contentfilter;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public final class ContentFilterTopology {
  private static final String DEFAULT_INPUT = "pattern.content-filter.in";
  private static final String DEFAULT_CLEAN = "pattern.content-filter.clean";
  private static final String DEFAULT_DROPPED = "pattern.content-filter.dropped";

  private ContentFilterTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<ContentEvent> eventSerde = JsonSerdes.serde(ContentEvent.class);

    String inputTopic = System.getProperty("input.topic", DEFAULT_INPUT);
    String cleanTopic = System.getProperty("clean.topic", DEFAULT_CLEAN);
    String droppedTopic = System.getProperty("dropped.topic", DEFAULT_DROPPED);

    Set<String> bannedWords =
        Arrays.stream(System.getProperty("filter.banned.words", "spam,fraud,malware").split(","))
            .map(String::trim)
            .filter(word -> !word.isEmpty())
            .map(word -> word.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    int maxLength = Integer.parseInt(System.getProperty("filter.max.length", "512"));

    KStream<String, ContentEvent> source =
        builder
            .stream(inputTopic, Consumed.with(Serdes.String(), eventSerde))
            .peek((key, value) -> metrics.markProcessed());

    KStream<String, ContentEvent>[] branches =
        source.branch(
            (key, value) -> shouldAccept(value, bannedWords, maxLength),
            (key, value) -> true);

    branches[0]
        .peek((key, value) -> metrics.markAccepted())
        .to(cleanTopic, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), eventSerde));

    branches[1]
        .peek((key, value) -> metrics.markDropped())
        .to(droppedTopic, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), eventSerde));

    return builder.build();
  }

  private static boolean shouldAccept(ContentEvent event, Set<String> bannedWords, int maxLength) {
    if (event == null || event.payload() == null) {
      return false;
    }
    String payload = event.payload();
    if (payload.length() > maxLength) {
      return false;
    }
    String lower = payload.toLowerCase(Locale.ROOT);
    return bannedWords.stream().noneMatch(lower::contains);
  }
}
