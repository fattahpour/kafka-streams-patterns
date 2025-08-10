package io.example.kstreamspatterns.joinkstreamktable;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
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
    void joinsWhenTableHasKey() {
        // Make the builder read the same topics the test writes to
        System.setProperty("stream.topic", "stream-join");
        System.setProperty("table.topic",  "table-join");
        System.setProperty("output.topic", "joined-join");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-ktable-join-unit-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> tableIn =
                    driver.createInputTopic("table-join", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String,String> streamIn =
                    driver.createInputTopic("stream-join", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("joined-join", Serdes.String().deserializer(), Serdes.String().deserializer());

            long t0 = Instant.now().toEpochMilli();
            // KTable update must exist at (or before) the stream record's timestamp
            tableIn.pipeInput("k", "T", t0);
            streamIn.pipeInput("k", "S", t0 + 1);

            List<String> values = out.readValuesToList();
            assertThat(values).containsExactly("S|T");
        }
    }

    @Test
    void dropsWhenTableMissingKey() {
        System.setProperty("stream.topic", "stream-join");
        System.setProperty("table.topic",  "table-join");
        System.setProperty("output.topic", "joined-join");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-ktable-join-unit-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> streamIn =
                    driver.createInputTopic("stream-join", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("joined-join", Serdes.String().deserializer(), Serdes.String().deserializer());

            long t0 = Instant.now().toEpochMilli();
            // No table value for key "k" at t0 => no join output
            streamIn.pipeInput("k", "S", t0);

            assertThat(out.isEmpty()).isTrue();
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("stream.topic");
        System.clearProperty("table.topic");
        System.clearProperty("output.topic");
    }
}
