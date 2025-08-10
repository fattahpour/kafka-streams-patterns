package com.fattahpour.kstreamspatterns.aggwindowhopping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AggWindowHoppingIT {

    @Test
    void endToEndAggregation() {
        // Match builder defaults
        System.setProperty("input.topic", "hopping-input");
        System.setProperty("output.topic", "hopping-output");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg-window-hopping-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"); // TopologyTestDriver

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("hopping-input", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("hopping-output", Serdes.String().deserializer(), Serdes.String().deserializer());

            // Hopping window: size=60s, advance=30s. Use aligned timestamps for determinism.
            long t0 = 0L;
            in.pipeInput("k1", "a", t0);             // affects window start=0
            in.pipeInput("k1", "b", t0 + 1_000L);    // still window start=0 -> count 2
            in.pipeInput("k1", "c", t0 + 60_000L);   // affects starts 30_000 and 60_000

            List<KeyValue<String, String>> actual = out.readKeyValuesToList();

            // Order is not guaranteed across overlapping windows; assert by contents.
            assertThat(actual)
                    .containsExactlyInAnyOrder(
                            KeyValue.pair("k1@0", "1"),
                            KeyValue.pair("k1@0", "2"),
                            KeyValue.pair("k1@30000", "1"),
                            KeyValue.pair("k1@60000", "1"));
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("input.topic");
        System.clearProperty("output.topic");
    }
}
