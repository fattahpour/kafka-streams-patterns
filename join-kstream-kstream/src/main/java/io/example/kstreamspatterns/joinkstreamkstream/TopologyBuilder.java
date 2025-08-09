package io.example.kstreamspatterns.joinkstreamkstream;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String left = System.getProperty("left.topic", "left-join");
    String right = System.getProperty("right.topic", "right-join");
    String output = System.getProperty("output.topic", "joined");
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> leftStream =
        builder.stream(left, Consumed.with(Serdes.String(), Serdes.String()));
    KStream<String, String> rightStream =
        builder.stream(right, Consumed.with(Serdes.String(), Serdes.String()));
    leftStream
        .join(
            rightStream,
            (l, r) -> l + ":" + r,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
        .to(output, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
