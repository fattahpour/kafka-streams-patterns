package io.example.kstreamspatterns.aggwindowtumbling;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AggWindowTumblingIT {

    @Test
    void endToEndAggregation() {
        System.setProperty("input.topic",  "tumbling-input");
        System.setProperty("output.topic", "tumbling-output");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg-window-tumbling-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> in =
                    driver.createInputTopic("tumbling-input", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("tumbling-output", Serdes.String().deserializer(), Serdes.String().deserializer());

            // Align to window boundary to avoid flakiness (window size = 5s)
            long t0 = 0L;
            in.pipeInput("k", "x", t0);           // window [0..4999] -> count 1
            in.pipeInput("k", "y", t0 + 1_000);   // same window -> count 2
            in.pipeInput("k", "z", t0 + 6_000);   // next window [5000..9999] -> count 1

            List<String> values = out.readValuesToList();
            assertThat(values).containsExactly("1", "2", "1");
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("input.topic");
        System.clearProperty("output.topic");
    }
}
