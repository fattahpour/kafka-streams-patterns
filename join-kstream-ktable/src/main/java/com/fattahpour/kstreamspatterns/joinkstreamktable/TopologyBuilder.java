package com.fattahpour.kstreamspatterns.joinkstreamktable;

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
        String streamTopic = System.getProperty("stream.topic", "stream");
        String tableTopic  = System.getProperty("table.topic",  "table");
        String outTopic    = System.getProperty("output.topic", "joined");

        StreamsBuilder b = new StreamsBuilder();

        KTable<String, String> table =
                b.table(tableTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> stream =
                b.stream(streamTopic, Consumed.with(Serdes.String(), Serdes.String()));

        stream.join(table, (l, r) -> l + "|" + r)
                .to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        return b.build();
    }
}
