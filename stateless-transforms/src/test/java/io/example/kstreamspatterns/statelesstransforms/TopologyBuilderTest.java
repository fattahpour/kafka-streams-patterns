package io.example.kstreamspatterns.statelesstransforms;

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
    void mapFilterFlatMap() {
        // topics must match the topology's sysprops/defaults
        System.setProperty("input.topic", "input-stateless");
        System.setProperty("output.topic", "output-stateless");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("input-stateless", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("output-stateless", Serdes.String().deserializer(), Serdes.String().deserializer());

            in.pipeInput("k", "hello world");
            assertThat(out.readValue()).isEqualTo("HELLO");
            assertThat(out.readValue()).isEqualTo("WORLD");
            assertThat(out.isEmpty()).isTrue();
        }
    }

    @AfterEach
    void clearSysProps() {
        System.clearProperty("input.topic");
        System.clearProperty("output.topic");
    }
}
