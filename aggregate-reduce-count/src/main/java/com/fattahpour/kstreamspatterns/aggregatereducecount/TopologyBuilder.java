package com.fattahpour.kstreamspatterns.aggregatereducecount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String input       = System.getProperty("input.topic",     "arc-input");
        String countOut    = System.getProperty("count.topic",     "arc-count");
        String reduceOut   = System.getProperty("reduce.topic",    "arc-reduce");
        String aggregateOut= System.getProperty("aggregate.topic", "arc-aggregate");

        StreamsBuilder builder = new StreamsBuilder();

        var stream  = builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()));
        var grouped = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        // COUNT -> Long store (explicit serdes)
        grouped
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .mapValues(Object::toString)
                .to(countOut, Produced.with(Serdes.String(), Serdes.String()));

        // REDUCE -> String store (combine values; here simple concatenation)
        grouped
                .reduce(
                        (agg, v) -> (agg == null || agg.isEmpty()) ? v : (agg + "," + v),
                        Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .to(reduceOut, Produced.with(Serdes.String(), Serdes.String()));

        // AGGREGATE -> String store (init + custom aggregator)
        grouped
                .aggregate(
                        () -> "", // initializer
                        (k, v, agg) -> (agg == null || agg.isEmpty()) ? v : (agg + "|" + v),
                        Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .to(aggregateOut, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
