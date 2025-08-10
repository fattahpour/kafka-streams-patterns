package com.fattahpour.kstreamspatterns.joinktablektable;

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

class JoinKTableKTableIT {

    @Test
    void endToEndJoin() {
        // Align topics with the builder (or change defaults there)
        System.setProperty("left.table.topic",  "left-table");
        System.setProperty("right.table.topic", "right-table");
        System.setProperty("output.topic",      "joined-table");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-join-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> leftIn  =
                    driver.createInputTopic("left-table",  Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String,String> rightIn =
                    driver.createInputTopic("right-table", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("joined-table", Serdes.String().deserializer(), Serdes.String().deserializer());

            long t0 = Instant.now().toEpochMilli();

            // Write one side, then the other (timestamps ascending). The second write triggers the join emission.
            leftIn.pipeInput("k", "L", t0);
            rightIn.pipeInput("k", "R", t0 + 1);

            List<String> values = out.readValuesToList();
            assertThat(values).hasSize(1);
            assertThat(values.get(0)).isEqualTo("L|R");
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("left.table.topic");
        System.clearProperty("right.table.topic");
        System.clearProperty("output.topic");
    }
}
