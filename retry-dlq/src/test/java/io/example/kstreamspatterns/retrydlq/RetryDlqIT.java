/*
package io.example.kstreamspatterns.retrydlq;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RetryDlqIT {

    @Test
    void routesThroughRetriesAndDlq() {
        System.setProperty("input.topic", "in");
        System.setProperty("retry1.topic", "r1");
        System.setProperty("retry2.topic", "r2");
        System.setProperty("dlq.topic", "dead");
        System.setProperty("output.topic", "out");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "retry-dlq-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String, String> r1 =
                    driver.createOutputTopic("r1", Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String, String> r2 =
                    driver.createOutputTopic("r2", Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String, String> dead =
                    driver.createOutputTopic("dead", Serdes.String().deserializer(), Serdes.String().deserializer());

            // success -> out
            in.pipeInput("k1", "ok");
            assertThat(out.readValue()).isEqualTo("ok");

            // retry1 -> r1
            in.pipeInput("k2", "retry1:foo");
            assertThat(r1.readValue()).isEqualTo("retry1:foo");

            // retry2 -> r2
            in.pipeInput("k3", "retry2:bar");
            assertThat(r2.readValue()).isEqualTo("retry2:bar");

            // fail -> dlq
            in.pipeInput("k4", "fail:baz");
            assertThat(dead.readValue()).isEqualTo("fail:baz");
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("input.topic");
        System.clearProperty("retry1.topic");
        System.clearProperty("retry2.topic");
        System.clearProperty("dlq.topic");
        System.clearProperty("output.topic");
    }
}
*/
