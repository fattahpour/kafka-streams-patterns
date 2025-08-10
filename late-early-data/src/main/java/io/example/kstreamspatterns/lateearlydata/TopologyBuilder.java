package io.example.kstreamspatterns.lateearlydata;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        // Test-friendly defaults; system properties still override these.
        final String input  = System.getProperty("input.topic",  "late-early-input");
        final String early  = System.getProperty("early.topic",  "late-early-early");
        final String output = System.getProperty("output.topic", "late-early-output");

        StreamsBuilder b = new StreamsBuilder();

        // 1-minute tumbling windows, NO grace (finals emit once stream-time passes window end)
        KTable<Windowed<String>, Long> counts =
                b.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                        .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // EARLY results: every update before the window closes
        counts.toStream()
                .map((Windowed<String> k, Long v) -> KeyValue.pair(k.key(), v))
                .to(early, Produced.with(Serdes.String(), Serdes.Long()));

        // FINAL results only when the window closes
        counts.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((Windowed<String> k, Long v) -> KeyValue.pair(k.key(), v))
                .to(output, Produced.with(Serdes.String(), Serdes.Long()));

        return b.build();
    }
}
