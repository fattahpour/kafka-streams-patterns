package com.fattahpour.kstreamspatterns.enrichmentktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String input  = System.getProperty("input.topic",  "orders");
        String table  = System.getProperty("table.topic",  "products");
        String output = System.getProperty("output.topic", "enriched");

        StreamsBuilder b = new StreamsBuilder();

        KTable<String, String> products =
                b.table(table, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> orders =
                b.stream(input, Consumed.with(Serdes.String(), Serdes.String()));

        orders.leftJoin(products, (orderVal, productVal) ->
                        orderVal + ":" + (productVal == null ? "null" : productVal))
                .to(output, Produced.with(Serdes.String(), Serdes.String()));

        return b.build();
    }
}
