package io.example.kstreamspatterns.joinktablektable;

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
    void joinsWhenBothTablesHaveKey() {
        System.setProperty("left.table.topic",  "table-a");
        System.setProperty("right.table.topic", "table-b");
        System.setProperty("output.topic",      "joined-ab");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-join-unit-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> a =
                    driver.createInputTopic("table-a", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String,String> b =
                    driver.createInputTopic("table-b", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("joined-ab", Serdes.String().deserializer(), Serdes.String().deserializer());

            long t0 = Instant.now().toEpochMilli();
            a.pipeInput("k", "A", t0);
            b.pipeInput("k", "B", t0 + 1); // second write triggers join emission

            List<String> vals = out.readValuesToList();
            assertThat(vals).containsExactly("A|B");
        }
    }

    @Test
    void noOutputWhenMissingKey() {
        System.setProperty("left.table.topic",  "table-a");
        System.setProperty("right.table.topic", "table-b");
        System.setProperty("output.topic",      "joined-ab");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-join-unit-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> a =
                    driver.createInputTopic("table-a", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("joined-ab", Serdes.String().deserializer(), Serdes.String().deserializer());

            a.pipeInput("k", "A"); // no record on table-b for key "k" -> no output
            assertThat(out.isEmpty()).isTrue();
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("left.table.topic");
        System.clearProperty("right.table.topic");
        System.clearProperty("output.topic");
    }
}
