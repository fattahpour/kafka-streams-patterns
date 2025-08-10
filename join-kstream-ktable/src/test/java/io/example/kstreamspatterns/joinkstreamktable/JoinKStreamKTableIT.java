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

class JoinKStreamKTableIT {

    @Test
    void endToEndJoin() {
        // Align with builder defaults
        System.setProperty("stream.topic", "stream");
        System.setProperty("table.topic",  "table");
        System.setProperty("output.topic", "joined");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-ktable-join-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> tableIn =
                    driver.createInputTopic("table", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String,String> streamIn =
                    driver.createInputTopic("stream", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("joined", Serdes.String().deserializer(), Serdes.String().deserializer());

            long t0 = Instant.now().toEpochMilli();

            // KTABLE FIRST (timestamp <= stream)
            tableIn.pipeInput("k", "T", t0);        // table value exists at t0
            streamIn.pipeInput("k", "S", t0 + 1);   // stream arrives after -> join emits

            List<String> values = out.readValuesToList();
            assertThat(values).containsExactly("S|T");
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("stream.topic");
        System.clearProperty("table.topic");
        System.clearProperty("output.topic");
    }
}
