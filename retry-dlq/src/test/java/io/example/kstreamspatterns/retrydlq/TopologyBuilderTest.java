/*
package io.example.kstreamspatterns.retrydlq;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

public class TopologyBuilderTest {
    @Test
    void routesThroughRetriesAndDlq() {
        System.setProperty("input.topic", "in");
        System.setProperty("retry1.topic", "r1");
        System.setProperty("retry2.topic", "r2");
        System.setProperty("dlq.topic", "dead");
        System.setProperty("output.topic", "out");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> in  =
                    driver.createInputTopic("in",  Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out  =
                    driver.createOutputTopic("out",  Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String,String> r1   =
                    driver.createOutputTopic("r1",   Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String,String> r2   =
                    driver.createOutputTopic("r2",   Serdes.String().deserializer(), Serdes.String().deserializer());
            TestOutputTopic<String,String> dead =
                    driver.createOutputTopic("dead", Serdes.String().deserializer(), Serdes.String().deserializer());

            in.pipeInput("k1","ok");          assertThat(out.readValue()).isEqualTo("ok");
            in.pipeInput("k2","retry1:foo");  assertThat(r1.readValue()).isEqualTo("retry1:foo");
            in.pipeInput("k3","retry2:bar");  assertThat(r2.readValue()).isEqualTo("retry2:bar");
            in.pipeInput("k4","fail:baz");    assertThat(dead.readValue()).isEqualTo("fail:baz");
        }
    }
}
*/
