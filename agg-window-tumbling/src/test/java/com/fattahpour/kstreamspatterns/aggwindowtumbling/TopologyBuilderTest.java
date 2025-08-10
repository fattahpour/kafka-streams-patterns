package com.fattahpour.kstreamspatterns.aggwindowtumbling;

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

class TopologyBuilderTest {

    @Test
    void countsPerWindow() {
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

            // Use a fixed, window-aligned base to avoid boundary flakiness
            long t0 = 0L;           // start of a 5s window
            in.pipeInput("k", "x", t0);          // -> count 1 (window [0..4999])
            in.pipeInput("k", "y", t0 + 1_000);  // -> count 2 (same window)
            in.pipeInput("k", "z", t0 + 6_000);  // -> count 1 (next window [5000..9999])

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
