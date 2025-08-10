package com.fattahpour.kstreamspatterns.materializedviews;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        // Match test defaults
        String input       = System.getProperty("input.topic",  "input-materialized");
        String output      = System.getProperty("output.topic", "output-materialized");
        String countsStore = System.getProperty("counts.store", "counts-store");
        String valuesStore = System.getProperty("values.store", "values-store");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
                builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()));

        // Latest value per key (materialized / queryable)
        source
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce(
                        (oldV, newV) -> newV,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(valuesStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()));

        // Count per key (materialized + emitted)
        source
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(countsStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()))
                .toStream()
                .to(output, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
