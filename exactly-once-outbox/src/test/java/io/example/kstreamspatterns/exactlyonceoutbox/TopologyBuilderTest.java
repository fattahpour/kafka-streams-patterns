package io.example.kstreamspatterns.exactlyonceoutbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TopologyBuilderTest {
    @Test
    void routesToProcessedAndOutbox() {
        System.setProperty("input.topic", "in");
        System.setProperty("processed.topic", "proc");
        System.setProperty("outbox.topic", "out");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        // Optional for EOS tests:
        // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String, String> input =
                    driver.createInputTopic("in", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> proc =
                    driver.createOutputTopic("proc", Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("k", "v");
            assertThat(proc.readValue()).isEqualTo("V");
            assertThat(out.readValue()).isEqualTo("V");
        }
    }

    @AfterEach
    void clearSysProps() {
        System.clearProperty("input.topic");
        System.clearProperty("processed.topic");
        System.clearProperty("outbox.topic");
    }
}
