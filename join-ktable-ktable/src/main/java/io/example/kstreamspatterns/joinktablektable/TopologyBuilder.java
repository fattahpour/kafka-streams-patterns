package io.example.kstreamspatterns.joinktablektable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public final class TopologyBuilder {
    private TopologyBuilder() {}

    public static Topology build() {
        String left   = System.getProperty("left.table.topic",  "left-table");
        String right  = System.getProperty("right.table.topic", "right-table");
        String output = System.getProperty("output.topic",      "joined-table");

        StreamsBuilder b = new StreamsBuilder();

        KTable<String, String> leftT  = b.table(left,  Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, String> rightT = b.table(right, Consumed.with(Serdes.String(), Serdes.String()));

        leftT.join(rightT, (l, r) -> l + "|" + r)
                .toStream() // important: emit the joined updates
                .to(output, Produced.with(Serdes.String(), Serdes.String()));

        return b.build();
    }
}
