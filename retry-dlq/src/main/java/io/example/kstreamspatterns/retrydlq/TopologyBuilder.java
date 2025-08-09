package io.example.kstreamspatterns.retrydlq;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

public final class TopologyBuilder {
  private TopologyBuilder() {}

  public static Topology build() {
    String input = System.getProperty("input.topic", "input-retry");
    String retry1 = System.getProperty("retry1.topic", "retry-1");
    String retry2 = System.getProperty("retry2.topic", "retry-2");
    String dlq = System.getProperty("dlq.topic", "dlq");
    String output = System.getProperty("output.topic", "output-retry");

    StreamsBuilder builder = new StreamsBuilder();
    String processorName = "retry-processor";
    builder
        .stream(
            Arrays.asList(input, retry1, retry2),
            Consumed.with(Serdes.String(), Serdes.String()))
        .process(RetryProcessor::new, Named.as(processorName));

    builder.addSink("success-sink", output, Serdes.String().serializer(), Serdes.String().serializer(), processorName);
    builder.addSink("retry1-sink", retry1, Serdes.String().serializer(), Serdes.String().serializer(), processorName);
    builder.addSink("retry2-sink", retry2, Serdes.String().serializer(), Serdes.String().serializer(), processorName);
    builder.addSink("dlq-sink", dlq, Serdes.String().serializer(), Serdes.String().serializer(), processorName);

    return builder.build();
  }

  static class RetryProcessor implements Processor<String, String> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
      this.context = context;
    }

    @Override
    public void process(String key, String value) {
      Headers headers = context.headers();
      int attempt = 0;
      Header header = headers.lastHeader("attempt");
      if (header != null) {
        attempt = Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
      }
      try {
        if ("fail".equals(value)) {
          throw new RuntimeException("boom");
        }
        context.forward(key, value.toUpperCase(), To.child("success-sink"));
      } catch (RuntimeException e) {
        int next = attempt + 1;
        headers.remove("attempt");
        headers.add("attempt", String.valueOf(next).getBytes(StandardCharsets.UTF_8));
        headers.remove("error");
        headers.add("error", e.getMessage().getBytes(StandardCharsets.UTF_8));
        String sink = next == 1 ? "retry1-sink" : next == 2 ? "retry2-sink" : "dlq-sink";
        context.forward(key, value, To.child(sink));
      }
    }

    @Override
    public void close() {}
  }
}
