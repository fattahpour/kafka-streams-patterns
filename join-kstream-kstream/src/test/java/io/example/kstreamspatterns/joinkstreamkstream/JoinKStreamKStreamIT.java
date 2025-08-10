package io.example.kstreamspatterns.joinkstreamkstream;

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

class JoinKStreamKStreamIT {

    @Test
    void endToEndJoin() {
        System.setProperty("left.topic",   "left");
        System.setProperty("right.topic",  "right");
        System.setProperty("output.topic", "joined");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> leftIn =
                    driver.createInputTopic("left", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String,String> rightIn =
                    driver.createInputTopic("right", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("joined", Serdes.String().deserializer(), Serdes.String().deserializer());

            long t0 = Instant.now().toEpochMilli();
            // within 10s window
            leftIn.pipeInput("k", "L", t0);
            rightIn.pipeInput("k", "R", t0 + 1000);

            List<String> values = out.readValuesToList();
            assertThat(values).hasSize(1);
            assertThat(values.get(0)).isEqualTo("L|R");
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("left.topic");
        System.clearProperty("right.topic");
        System.clearProperty("output.topic");
    }
}
