package com.fattahpour.kstreamspatterns.enrichmentktable;

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

class EnrichmentKTableIT {

    @Test
    void endToEndEnrichment() {
        System.setProperty("input.topic",  "enrich-input");
        System.setProperty("table.topic",  "enrich-table");
        System.setProperty("output.topic", "enrich-output");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrich-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String,String> tableIn =
                    driver.createInputTopic("enrich-table", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String,String> streamIn =
                    driver.createInputTopic("enrich-input", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String,String> out =
                    driver.createOutputTopic("enrich-output", Serdes.String().deserializer(), Serdes.String().deserializer());

            long t0 = Instant.now().toEpochMilli();
            // 1) table first (<= stream timestamp)
            tableIn.pipeInput("apple", "5", t0 + 1);
            // 2) then stream
            streamIn.pipeInput("apple", "apple", t0 + 2);

            List<String> values = out.readValuesToList();
            assertThat(values).contains("apple:5");
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("input.topic");
        System.clearProperty("table.topic");
        System.clearProperty("output.topic");
    }
}
