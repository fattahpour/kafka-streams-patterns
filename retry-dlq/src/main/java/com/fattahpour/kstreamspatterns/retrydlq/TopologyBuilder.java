package com.fattahpour.kstreamspatterns.retrydlq;

import java.util.Arrays;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String input  = System.getProperty("input.topic",  "input-retry");
        String retry1 = System.getProperty("retry1.topic", "retry-1");
        String retry2 = System.getProperty("retry2.topic", "retry-2");
        String dlq    = System.getProperty("dlq.topic",    "dlq");
        String output = System.getProperty("output.topic", "output-retry");

        StreamsBuilder builder = new StreamsBuilder();

        builder
                .stream(
                        Arrays.asList(input, retry1, retry2),
                        Consumed.with(Serdes.String(), Serdes.String()))
                .split(Named.as("route-"))
                .branch((k, v) -> v == null || v.startsWith("fail:"),
                        Branched.withConsumer(ks -> ks.to(dlq, Produced.with(Serdes.String(), Serdes.String()))))
                .branch((k, v) -> v != null && v.startsWith("retry2:"),
                        Branched.withConsumer(ks -> ks.to(retry2, Produced.with(Serdes.String(), Serdes.String()))))
                .branch((k, v) -> v != null && v.startsWith("retry1:"),
                        Branched.withConsumer(ks -> ks.to(retry1, Produced.with(Serdes.String(), Serdes.String()))))
                .defaultBranch(
                        Branched.withConsumer(ks -> ks.to(output, Produced.with(Serdes.String(), Serdes.String()))));

        return builder.build();
    }
}
