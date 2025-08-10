package io.example.kstreamspatterns.aggregatereducecount;

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
    void aggregateReduceCount() {
        // Use the topic names this test writes to
        System.setProperty("input.topic", "input-arc");
        System.setProperty("count.topic", "count-arc");
        System.setProperty("reduce.topic", "reduce-arc");
        System.setProperty("aggregate.topic", "aggregate-arc");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "arc-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("input-arc", Serdes.String().serializer(), Serdes.String().serializer());

            TestOutputTopic<String, String> countOut =
                    driver.createOutputTopic("count-arc", Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String, String> reduceOut =
                    driver.createOutputTopic("reduce-arc", Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String, String> aggregateOut =
                    driver.createOutputTopic("aggregate-arc", Serdes.String().deserializer(), Serdes.String().deserializer());

            // two events for same key
            in.pipeInput("k", "a");
            in.pipeInput("k", "b");

            // COUNT emits "1", then "2"
            List<String> counts = countOut.readValuesToList();
            assertThat(counts).containsExactly("1", "2");

            // REDUCE concatenates with comma: "a", then "a,b"
            List<String> reduced = reduceOut.readValuesToList();
            assertThat(reduced).containsExactly("a", "a,b");

            // AGGREGATE concatenates with pipe: "a", then "a|b"
            List<String> aggregated = aggregateOut.readValuesToList();
            assertThat(aggregated).containsExactly("a", "a|b");
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("input.topic");
        System.clearProperty("count.topic");
        System.clearProperty("reduce.topic");
        System.clearProperty("aggregate.topic");
    }
}
