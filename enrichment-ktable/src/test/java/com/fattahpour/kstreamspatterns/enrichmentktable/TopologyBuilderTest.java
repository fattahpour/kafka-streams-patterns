package com.fattahpour.kstreamspatterns.enrichmentktable;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
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
    void enrichesStreamWithTable() {
        // Match the defaults in TopologyBuilder
        System.setProperty("input.topic",  "orders");
        System.setProperty("table.topic",  "products");
        System.setProperty("output.topic", "enriched");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dummy-topology-test-driver-app-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> products =
                    driver.createInputTopic("products", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String,String> orders =
                    driver.createInputTopic("orders", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> enriched =
                    driver.createOutputTopic("enriched", Serdes.String().deserializer(), Serdes.String().deserializer());

            long t0 = Instant.now().toEpochMilli();
            // table first, then stream (table ts <= stream ts)
            products.pipeInput("apple", "5", t0 + 1);
            orders.pipeInput("apple", "apple", t0 + 2);

            assertThat(enriched.readValuesToList()).contains("apple:5");
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("input.topic");
        System.clearProperty("table.topic");
        System.clearProperty("output.topic");
    }
}
