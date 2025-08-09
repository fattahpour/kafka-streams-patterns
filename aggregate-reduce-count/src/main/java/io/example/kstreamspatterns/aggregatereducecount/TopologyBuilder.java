package io.example.kstreamspatterns.aggregatereducecount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "input-arc");
    String sum = System.getProperty("sum.topic", "sum-arc");
    String max = System.getProperty("max.topic", "max-arc");
    String count = System.getProperty("count.topic", "count-arc");

    StreamsBuilder builder = new StreamsBuilder();
    var grouped =
        builder
            .stream(input, Consumed.with(Serdes.String(), Serdes.Long()))
            .groupByKey();
    grouped.aggregate(() -> 0L, (k, v, agg) -> agg + v).toStream().to(sum);
    grouped.reduce(Long::max).toStream().to(max);
    grouped.count().toStream().to(count);
    return builder.build();
  }
}
