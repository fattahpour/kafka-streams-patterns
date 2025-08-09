package io.example.kstreamspatterns.enrichmentktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String orders = System.getProperty("orders.topic", "orders");
    String products = System.getProperty("products.topic", "products");
    String output = System.getProperty("output.topic", "enriched-orders");
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> orderStream =
        builder.stream(orders, Consumed.with(Serdes.String(), Serdes.String()));
    KTable<String, String> productTable =
        builder.table(products, Consumed.with(Serdes.String(), Serdes.String()));
    orderStream
        .leftJoin(
            productTable,
            (order, name) -> name == null ? "unknown:" + order : name + ":" + order)
        .to(output, org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }
}
