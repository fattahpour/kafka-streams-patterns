package io.example.kstreamspatterns.suppression;

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

class SuppressionIT {

    @Test
    void emitsOnlyFinalResults() {
        System.setProperty("input.topic",  "suppress-input");
        System.setProperty("output.topic", "suppress-output");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "suppression-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> in =
                    driver.createInputTopic("suppress-input", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,Long> out =
                    driver.createOutputTopic("suppress-output", Serdes.String().deserializer(), Serdes.Long().deserializer());

            long t0 = 0L;              // window [0..4999]
            in.pipeInput("k", "a", t0);            // count=1 (suppressed)
            in.pipeInput("k", "b", t0 + 1_000L);   // count=2 (still suppressed)

            // Advance stream-time >= window end to flush final result
            in.pipeInput("z", "noop", t0 + 5_000L); // closes prior window, no output for "z" yet

            List<Long> values = out.readValuesToList();
            assertThat(values).containsExactly(2L);
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("input.topic");
        System.clearProperty("output.topic");
    }
}
