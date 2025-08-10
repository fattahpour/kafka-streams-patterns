package com.fattahpour.kstreamspatterns.joinkstreamkstream;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String left   = System.getProperty("left.topic",   "left");
        String right  = System.getProperty("right.topic",  "right");
        String output = System.getProperty("output.topic", "joined");

        StreamsBuilder b = new StreamsBuilder();

        KStream<String, String> lhs = b.stream(left,  Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> rhs = b.stream(right, Consumed.with(Serdes.String(), Serdes.String()));

        lhs.join(
                        rhs,
                        (l, r) -> l + "|" + r,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
                        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
                .to(output, Produced.with(Serdes.String(), Serdes.String()));

        return b.build();
    }
}
