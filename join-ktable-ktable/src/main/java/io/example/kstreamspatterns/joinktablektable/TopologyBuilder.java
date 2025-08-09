package io.example.kstreamspatterns.joinktablektable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String tableA = System.getProperty("tableA.topic", "table-a");
    String tableB = System.getProperty("tableB.topic", "table-b");
    String output = System.getProperty("output.topic", "joined");
    StreamsBuilder builder = new StreamsBuilder();
    KTable<String, String> a = builder.table(tableA, Consumed.with(Serdes.String(), Serdes.String()));
    KTable<String, String> b = builder.table(tableB, Consumed.with(Serdes.String(), Serdes.String()));
    a.join(b, (va, vb) -> va + ":" + vb)
        .toStream()
        .to(output, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
