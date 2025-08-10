package com.fattahpour.kstreamspatterns.exactlyonceoutbox;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String input = System.getProperty("input.topic", "orders");
        String processed = System.getProperty("processed.topic", "processed-orders");
        String outbox = System.getProperty("outbox.topic", "orders-outbox");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream =
                builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                        .mapValues(v -> v == null ? null : v.toUpperCase());

        stream.to(processed, Produced.with(Serdes.String(), Serdes.String()));
        stream.to(outbox, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }
}
