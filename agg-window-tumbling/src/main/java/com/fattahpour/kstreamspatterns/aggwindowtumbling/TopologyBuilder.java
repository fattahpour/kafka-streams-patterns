package com.fattahpour.kstreamspatterns.aggwindowtumbling;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String input  = System.getProperty("input.topic",  "tumbling-input");
        String output = System.getProperty("output.topic", "tumbling-output");

        StreamsBuilder b = new StreamsBuilder();

        b.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(5), Duration.ZERO))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((Windowed<String> k, Long v) -> KeyValue.pair(k.key() + "@" + k.window().start(), v.toString()))
                .to(output, Produced.with(Serdes.String(), Serdes.String()));

        return b.build();
    }
}
