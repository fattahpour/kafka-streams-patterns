package io.example.kstreamspatterns.aggwindowsession;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String input  = System.getProperty("input.topic",  "session-input");
        String output = System.getProperty("output.topic", "session-output");

        StreamsBuilder b = new StreamsBuilder();

        b.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(5)))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("session-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withCachingDisabled())
                .toStream()
                .filter((k, v) -> v != null) // tombstone updates during session merge/close
                .map((Windowed<String> k, Long v) ->
                        KeyValue.pair(k.key() + "@" + k.window().start(), v.toString()))
                .to(output, Produced.with(Serdes.String(), Serdes.String()));

        return b.build();
    }
}
