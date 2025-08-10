package io.example.kstreamspatterns.statelesstransforms;

import java.util.Arrays;
import java.util.Locale;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String input  = System.getProperty("input.topic", "input-stateless");
        String output = System.getProperty("output.topic", "output-stateless");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream =
                builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                        // example stateless steps
                        .filter((k, v) -> v != null && !v.isBlank())
                        .mapValues(v -> v.toUpperCase(Locale.ROOT))
                        .flatMapValues(v -> Arrays.asList(v.split("\\s+"))); // split words

        stream.to(output, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }
}
